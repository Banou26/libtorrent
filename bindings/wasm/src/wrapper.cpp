// libtorrent — minimal WASM C API
//
// The shape of the bridge:
//
//   JS owns sockets (via @fkn/lib net/dgram) and storage (OPFS or app-supplied).
//   WASM owns the BT state machine, hashing, peer logic.
//
//   Every C call here is non-blocking. The JS event loop calls lt_session_tick()
//   when something changes (data arrived, timer fired, JS pushed a disk
//   completion). The C++ side runs io_context.poll(), processes whatever is
//   ready, and returns. No Asyncify.

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#ifdef __EMSCRIPTEN__
// Emscripten ships __syscall_setsockopt as a `weak` C stub in
// emscripten_syscall_stubs.c that always returns -ENOPROTOOPT and prints
// "warning: unsupported syscall: __syscall_setsockopt" once per call.
// Boost.Asio reads that as "this socket can't be configured" and refuses
// to write the BT handshake — that's why our 19 connected TCP fds had
// recv=0/send=0 across the board. Override with a strong symbol that
// silently accepts everything. We don't have a real kernel here; the
// rate-limit / NODELAY / KEEPALIVE knobs are no-ops over the WebVPN
// tunnel anyway.
extern "C" int __syscall_setsockopt(int /*fd*/, int /*level*/, int /*optname*/,
                                    int /*optval*/, int /*optlen*/, int /*dummy*/) {
  return 0;
}
#endif

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/magnet_uri.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/alert.hpp"
#include "libtorrent/torrent_status.hpp"
#include "libtorrent/torrent_info.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/sha1_hash.hpp"
#include "libtorrent/hex.hpp"
#include "libtorrent/info_hash.hpp"

#include "disk_io.hpp"

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#define LT_API extern "C" EMSCRIPTEN_KEEPALIVE
#define LT_LOG(msg) emscripten_log(EM_LOG_CONSOLE, "%s", msg)
#else
#define LT_API extern "C"
#define LT_LOG(msg) ((void)0)
#endif

namespace {

struct session_state {
  std::unique_ptr<lt::io_context> ioc;
  std::unique_ptr<lt::session> ses;

  // torrent_handle is heavy and not trivially copyable across the C boundary;
  // we hand out small u32 ids and keep the real handles here.
  std::unordered_map<std::uint32_t, lt::torrent_handle> handles;
  std::uint32_t next_handle_id = 1;
};

session_state* g_session = nullptr;

// Alerts get serialized into a length-prefixed binary stream so JS can pull
// them in one shot per tick without bouncing back and forth. Each record is:
//
//   u32 type        (alert id, matches libtorrent's alert::type())
//   u32 size        (size of payload that follows, in bytes)
//   <payload>       (UTF-8 text for now — same as alert::message())
//
// Keeping this binary instead of JSON avoids the JSON-encode cost on hot
// alerts like block_finished_alert.
struct alert_buffer {
  std::vector<std::uint8_t> data;
  void put_u32(std::uint32_t v) {
    auto p = reinterpret_cast<std::uint8_t const*>(&v);
    data.insert(data.end(), p, p + 4);
  }
  void put_bytes(void const* src, std::size_t n) {
    auto p = static_cast<std::uint8_t const*>(src);
    data.insert(data.end(), p, p + n);
  }
};

alert_buffer g_pending_alerts;

std::uint32_t register_handle(lt::torrent_handle h) {
  if (!h.is_valid()) return 0;
  auto id = g_session->next_handle_id++;
  g_session->handles.emplace(id, std::move(h));
  return id;
}

lt::torrent_handle* lookup_handle(std::uint32_t id) {
  auto it = g_session->handles.find(id);
  return it == g_session->handles.end() ? nullptr : &it->second;
}

} // namespace

// ---- session lifecycle -----------------------------------------------------

LT_API int lt_session_create() {
  if (g_session) return -1;

  g_session = new session_state();
  g_session->ioc = std::make_unique<lt::io_context>();

  lt::settings_pack sp;
  // Without at least one listen interface libtorrent has no listen_socket_t
  // to use as the source for outgoing tracker/peer connects, so the entire
  // network side stays idle. The bind itself goes through our JS shim
  // (which returns success even if the WebVPN can't actually accept
  // inbound), but the listen_socket_t entry is what tracker/UDP paths
  // attach themselves to.
  sp.set_str(lt::settings_pack::listen_interfaces, "0.0.0.0:6882");
  sp.set_bool(lt::settings_pack::enable_upnp, false);
  sp.set_bool(lt::settings_pack::enable_natpmp, false);
  sp.set_bool(lt::settings_pack::enable_lsd, false);
  // Throughput tuning. Browser environment: we have no kernel TCP buffers
  // to feed into and our recv loop is paced by JS task scheduling rather
  // than a real OS reactor, so the defaults (which assume a Linux box
  // with native sockets) under-utilise what we can actually do.
  //   - send/recv buffer watermarks: bump to keep more bytes in flight
  //     between request and reply, especially helpful over the WebVPN
  //     where the round-trip is iframe-relayed.
  //   - max_out_request_queue: cap the per-peer outstanding request
  //     count so a single laggy peer doesn't queue up MBs of work that
  //     we won't process this second.
  sp.set_int(lt::settings_pack::send_buffer_watermark, 5 * 1024 * 1024);
  sp.set_int(lt::settings_pack::send_buffer_low_watermark, 512 * 1024);
  sp.set_int(lt::settings_pack::send_buffer_watermark_factor, 150);
  // max_out_request_queue caps in-flight piece requests per peer. At
  // 20+ MiB/s × default request_queue_time of 3s × 16 KiB blocks, the
  // desired queue size hits ~4000 — old 1500 triggered the
  // outstanding_request_limit_reached performance warning right before
  // peers got "snubbed" because we couldn't keep them fed. Lift it.
  sp.set_int(lt::settings_pack::max_out_request_queue, 5000);
  sp.set_int(lt::settings_pack::connections_limit, 500);
  // Keep peers from being declared "snubbed" while we're processing a
  // burst — defaults assume ~100ms response latency; our JS tick chain
  // can stretch that under heavy load.
  sp.set_int(lt::settings_pack::peer_timeout, 240);
  sp.set_int(lt::settings_pack::request_timeout, 120);
  // Speed up peer selection — defaults bias for long-running clients.
  sp.set_int(lt::settings_pack::unchoke_slots_limit, 32);
  // The hot path is data movement, not bookkeeping. Disable the rate
  // smoothing that introduces small artificial waits.
  sp.set_int(lt::settings_pack::mixed_mode_algorithm, lt::settings_pack::prefer_tcp);
  // Re-enable uTP for real-world tests — public swarms have a mix of TCP and
  // uTP peers, and many seeders are uTP-first. prefer_tcp above already gives
  // TCP priority when both are available. uTP path caps at ~14 MiB/s due to
  // LEDBAT delay sensitivity but is still much better than no peer.
  // DHT bootstraps via DNS to router.bittorrent.com / utorrent.com which
  // would spawn a resolver worker thread — fails hard under -sUSE_PTHREADS=0.
  // Disable for now; can be re-enabled once the JS-side DNS path is wired.
  sp.set_bool(lt::settings_pack::enable_dht, false);
  // Force the disk/hashing pools to size 0 so nothing tries pthread_create.
  // Our wasm_disk_io ignores these anyway, but settings_pack init paths
  // may still touch them.
  sp.set_int(lt::settings_pack::aio_threads, 0);
  sp.set_int(lt::settings_pack::hashing_threads, 0);
  // Pull the production-relevant categories. The *_log categories
  // (session_log, torrent_log, peer_log, dht_log, picker_log) were on
  // earlier for diagnosis but each emits dozens of message-rich alerts
  // per tick once a torrent is active — the alert queue grows, every
  // call to pop_alerts walks more entries, and the JS thread spends
  // most of its time draining them. Off by default; flip via
  // lt_session_set_log_verbose() when actively debugging.
  sp.set_int(lt::settings_pack::alert_mask,
      lt::alert_category::error
    | lt::alert_category::peer
    | lt::alert_category::port_mapping
    | lt::alert_category::storage
    | lt::alert_category::tracker
    | lt::alert_category::status
    | lt::alert_category::ip_block
    | lt::alert_category::performance_warning
    | lt::alert_category::dht
    | lt::alert_category::stats);

  lt::session_params params(sp);
  params.disk_io_constructor = lt::wasm_disk_io_constructor;

  // Hand the session our io_context so we can drive .poll() from tick.
  // This is the single-threaded path; the session will not spawn its own
  // network thread.
  LT_LOG("[lt] constructing session…");
  g_session->ses = std::make_unique<lt::session>(std::move(params), *g_session->ioc);
  LT_LOG("[lt] session constructed");

  // Some session_impl init paths still try to spawn a worker thread (e.g.
  // boost.asio's resolver service, ip_change_notifier on Linux), which
  // fails under -sUSE_PTHREADS=0. session_impl::wrap() catches the
  // resulting system_error and calls pause(). Resume immediately so the
  // session actually does work — the failed thread spawn doesn't break
  // anything else, it just means the corresponding optional facility (DNS
  // worker, NIC-change watcher) is unavailable.
  g_session->ses->resume();

  return 0;
}

LT_API void lt_session_destroy() {
  if (!g_session) return;
  g_session->handles.clear();
  g_session->ses.reset();
  g_session->ioc.reset();
  g_pending_alerts.data.clear();
  delete g_session;
  g_session = nullptr;
}

// Run all currently-ready handlers and return. Never blocks.
//
// JS schedules subsequent ticks when something becomes ready:
//   - new bytes arrive on a TCP/UDP socket
//   - a disk job completes
//   - a timer expires
// Combined with non-blocking sockets, this gives us an event-driven loop
// without Asyncify.
// Tick stats — readable via lt_diag_*().
static std::int64_t g_tick_count = 0;
static std::int64_t g_total_handlers = 0;

LT_API std::int64_t lt_diag_tick_count() { return g_tick_count; }
LT_API std::int64_t lt_diag_total_handlers() { return g_total_handlers; }

// Direct test: open a TCP socket via Asio (bypasses libtorrent). If FKN_socket
// fires after this, the bridge works and the issue is in libtorrent's
// listen-socket setup. If it doesn't fire, the issue is Asio/Emscripten's
// socket service.
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
LT_API int lt_diag_open_tcp() {
  try {
    if (!g_session) return -1;
    boost::asio::ip::tcp::socket s(*g_session->ioc);
    boost::system::error_code ec;
    s.open(boost::asio::ip::tcp::v4(), ec);
    if (ec) return -ec.value();
    int fd = s.native_handle();
    s.close(ec);
    return fd;
  } catch (std::exception const& e) {
    return -9999;
  }
}

// Full chain: acceptor.open + bind + listen — exactly what session_impl does
// in setup_listener.
LT_API int lt_diag_listen_full() {
  if (!g_session) return -1;
  try {
    boost::asio::ip::tcp::acceptor acc(*g_session->ioc);
    boost::system::error_code ec;
    acc.open(boost::asio::ip::tcp::v4(), ec);
    if (ec) return -100 - ec.value();
    acc.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true), ec);
    boost::asio::ip::tcp::endpoint ep(boost::asio::ip::address_v4::any(), 6881);
    acc.bind(ep, ec);
    if (ec) return -200 - ec.value();
    acc.listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) return -300 - ec.value();
    int fd = acc.native_handle();
    acc.close(ec);
    return fd;
  } catch (std::exception const& e) {
    return -9999;
  }
}

LT_API int lt_diag_open_udp() {
  try {
    if (!g_session) return -1;
    boost::asio::ip::udp::socket s(*g_session->ioc);
    boost::system::error_code ec;
    s.open(boost::asio::ip::udp::v4(), ec);
    if (ec) return -ec.value();
    int fd = s.native_handle();
    s.close(ec);
    return fd;
  } catch (std::exception const& e) {
    return -9999;
  }
}

// Trigger a listen_port query — this round-trips through the io_context.
LT_API int lt_diag_listen_port() {
  if (!g_session) return -1;
  return g_session->ses->listen_port();
}

// Force reopen_listen_sockets by posting a new settings update.
LT_API void lt_diag_force_reopen() {
  if (!g_session) return;
  lt::settings_pack sp;
  sp.set_str(lt::settings_pack::listen_interfaces, "0.0.0.0:6882");
  g_session->ses->apply_settings(std::move(sp));
}

// Test the parse: returns how many interfaces parse_listen_interfaces
// produced for the given string, plus push errors into the alert stream.
#include "libtorrent/string_util.hpp"
LT_API int lt_diag_parse_interfaces(char const* str) {
  if (!str) return -1;
  std::vector<std::string> errors;
  auto ifaces = lt::parse_listen_interfaces(std::string(str), errors);
  for (auto const& e : errors) {
    std::string msg = "parse-error: " + e;
    g_pending_alerts.put_u32(0xFFFFFFF0u);
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(msg.size()));
    g_pending_alerts.put_bytes(msg.data(), msg.size());
  }
  // Echo result via alerts.
  for (auto const& i : ifaces) {
    std::string msg = "iface: device=" + i.device + " port=" + std::to_string(i.port)
                    + " ssl=" + (i.ssl ? "1" : "0") + " local=" + (i.local ? "1" : "0");
    g_pending_alerts.put_u32(0xFFFFFFF1u);
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(msg.size()));
    g_pending_alerts.put_bytes(msg.data(), msg.size());
  }
  return static_cast<int>(ifaces.size());
}

// Per-tick handler budget. io_context::poll() would otherwise drain the
// entire ready queue in one synchronous call — when libtorrent kicks off a
// torrent it posts hundreds of handlers at once and a single tick can pin
// the main thread for 100s of ms. Capping forces JS to get control back
// between batches; the JS side rearms scheduleTick when this returns the
// budget cap (meaning more work likely waiting).
//
// Drain handlers in a time-budgeted loop: keep calling poll() (which
// processes all currently-ready handlers in one shot) as long as more
// work appears, capped at ~8ms to leave the renderer breathing room.
// Without the loop, work that becomes ready DURING the tick (e.g. a
// handler that posts another handler) has to wait a full JS task round-
// trip to be picked up. Browser task rate is ~150-200/sec under load
// so each spared round-trip is worth it.
LT_API int lt_session_tick() {
  if (!g_session) return 0;
  std::size_t ran = 0;
  try {
    auto const start = std::chrono::steady_clock::now();
    // Worker variant: libtorrent runs in a dedicated Worker, so the
    // renderer never sees this tick. The only thing it shares time with
    // is the @fkn/lib dgram socket's 'message' handler. A 100 ms cap is
    // generous enough to let libtorrent burn down a full burst of
    // pending blocks without ping-pong; smaller budgets bounce control
    // back to JS between tiny batches, capping throughput well below
    // what the network is feeding us.
    auto const deadline = start + std::chrono::milliseconds(100);
    while (true) {
      std::size_t const n = g_session->ioc->poll();
      ran += n;
      if (n == 0) break;
      if (std::chrono::steady_clock::now() > deadline) break;
    }
    g_total_handlers += static_cast<std::int64_t>(ran);
    ++g_tick_count;
    // 1-second-window stats so we can see where wallclock goes:
    //   tick_us / handlers_processed / ticks_in_window
    static auto window_start = start;
    static std::int64_t window_tick_us = 0;
    static std::int64_t window_ticks = 0;
    static std::int64_t window_handlers = 0;
    auto const tick_end = std::chrono::steady_clock::now();
    window_tick_us += std::chrono::duration_cast<std::chrono::microseconds>(tick_end - start).count();
    window_ticks++;
    window_handlers += static_cast<std::int64_t>(ran);
    auto const since_window = std::chrono::duration_cast<std::chrono::seconds>(tick_end - window_start).count();
    if (since_window >= 1) {
      LT_LOG((std::string("[tick] ticks/s=") + std::to_string(window_ticks)
        + " handlers/s=" + std::to_string(window_handlers)
        + " busy_ms=" + std::to_string(window_tick_us / 1000)
        + " avg_us/tick=" + std::to_string(window_ticks ? window_tick_us / window_ticks : 0)
      ).c_str());
      window_start = tick_end;
      window_tick_us = 0;
      window_ticks = 0;
      window_handlers = 0;
    }
  } catch (std::system_error const& e) {
    std::string m = std::string("tick syserr: ") + e.what()
      + " | code=" + std::to_string(e.code().value())
      + " | category=" + e.code().category().name();
    g_pending_alerts.put_u32(0xFFFFFFFFu);
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(m.size()));
    g_pending_alerts.put_bytes(m.data(), m.size());
  } catch (std::exception const& e) {
    std::string m = std::string("tick exc: ") + e.what();
    g_pending_alerts.put_u32(0xFFFFFFFEu);
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(m.size()));
    g_pending_alerts.put_bytes(m.data(), m.size());
  }
  // restart() clears the stopped flag so the next poll() will work again.
  g_session->ioc->restart();
  return static_cast<int>(ran);
}

// Returns ms until the next libtorrent-internal timer would fire, capped at
// `max_ms`. JS uses this as the setTimeout fallback when no socket/disk
// activity is pumping ticks. -1 means "no upcoming timer" (use a long sleep).
LT_API int lt_session_next_timer_ms(int max_ms) {
  if (!g_session) return -1;
  // boost::asio doesn't directly expose "time to next timer". The cheap
  // approximation is to ask the session to post a status update on the next
  // tick (it will, internally, ping any expired timers). For now we surface
  // a fixed upper bound; future iterations can wire in the precise value.
  (void)max_ms;
  return 250;
}

// ---- alerts ---------------------------------------------------------------

LT_API void lt_session_pump_alerts() {
  if (!g_session) return;
  std::vector<lt::alert*> alerts;
  g_session->ses->pop_alerts(&alerts);

  for (auto* a : alerts) {
    std::string msg = a->message();
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(a->type()));
    g_pending_alerts.put_u32(static_cast<std::uint32_t>(msg.size()));
    g_pending_alerts.put_bytes(msg.data(), msg.size());

    // When a torrent is added, libtorrent posts an add_torrent_alert
    // carrying the handle. That's the only place we learn about handles
    // for things we didn't add through our wrapper (e.g. resume data).
    if (auto* ata = lt::alert_cast<lt::add_torrent_alert>(a)) {
      register_handle(ata->handle);
    }
  }
}

LT_API std::uint32_t lt_alerts_size() {
  return static_cast<std::uint32_t>(g_pending_alerts.data.size());
}

LT_API std::uint8_t const* lt_alerts_data() {
  return g_pending_alerts.data.data();
}

LT_API void lt_alerts_clear() {
  g_pending_alerts.data.clear();
}

// ---- torrents -------------------------------------------------------------

// Add a torrent from a magnet URI. Returns 0 on success; the actual handle
// arrives via add_torrent_alert (and pump_alerts registers it). Returning
// 0/!=0 instead of the handle is the price we pay for the single-threaded
// model: session_handle::add_torrent is a blocking sync_call that waits on
// a condition variable for the io_context to process the dispatched lambda,
// but our io_context only runs when JS calls _lt_session_tick(). So we
// MUST use async_add_torrent — anything else deadlocks instantly.
LT_API int lt_session_add_magnet(char const* magnet, char const* save_path) {
  if (!g_session || !magnet) return -1;
  lt::error_code ec;
  lt::add_torrent_params atp = lt::parse_magnet_uri(magnet, ec);
  if (ec) return -2;
  atp.save_path = save_path ? save_path : ".";
  g_session->ses->async_add_torrent(std::move(atp));
  return 0;
}

// Add a torrent from a .torrent file buffer. Same async semantics as above.
LT_API int lt_session_add_torrent_file(
    std::uint8_t const* buf, std::uint32_t len, char const* save_path) {
  if (!g_session || !buf || !len) return -1;
  lt::error_code ec;
  auto ti = std::make_shared<lt::torrent_info>(
      reinterpret_cast<char const*>(buf), static_cast<int>(len), ec);
  if (ec) return -2;
  lt::add_torrent_params atp;
  atp.ti = std::move(ti);
  atp.save_path = save_path ? save_path : ".";
  g_session->ses->async_add_torrent(std::move(atp));
  return 0;
}

LT_API int lt_session_remove_torrent(std::uint32_t id) {
  if (!g_session) return -1;
  auto* h = lookup_handle(id);
  if (!h) return -1;
  g_session->ses->remove_torrent(*h);
  g_session->handles.erase(id);
  return 0;
}

// Returns 0 on success and fills `out`. Layout matches js/index.ts.
// Kept POD-flat so JS can read it as a struct via DataView.
struct torrent_status_out {
  std::int32_t  state;
  std::int32_t  paused;
  float         progress;
  std::int64_t  total_download;
  std::int64_t  total_upload;
  std::int64_t  total_done;
  std::int64_t  total_wanted;
  std::int64_t  total_payload_download;
  std::int64_t  total_payload_upload;
  std::int32_t  download_rate;
  std::int32_t  upload_rate;
  std::int32_t  download_payload_rate;
  std::int32_t  upload_payload_rate;
  std::int32_t  num_peers;
  std::int32_t  num_seeds;
  std::int32_t  num_pieces;
  std::int32_t  num_connections;
  std::int32_t  has_metadata;
};

// Status query — non-blocking. Requests a status update; the result arrives
// as a state_update_alert that JS can pull through pump_alerts. We can't
// call h->status() directly here because it's a sync_call that waits on a
// condition variable for the io_context to process the dispatched lambda,
// and our io_context only runs when JS ticks (single-threaded, external
// io_context). All synchronous session APIs in libtorrent have this
// constraint.
LT_API int lt_torrent_post_status(std::uint32_t id) {
  if (!g_session) return -1;
  auto* h = lookup_handle(id);
  if (!h || !h->is_valid()) return -1;
  h->post_status();
  return 0;
}

// Stub kept for ABI compatibility; always returns -1 in the async model.
LT_API int lt_torrent_status(std::uint32_t id, torrent_status_out* out) {
  (void)id; (void)out;
  return -1;
}

// Hex-encoded infohash; writes up to 41 bytes (40 hex + NUL) into `out`.
LT_API int lt_torrent_infohash(std::uint32_t id, char* out) {
  if (!g_session || !out) return -1;
  auto* h = lookup_handle(id);
  if (!h || !h->is_valid()) return -1;
  auto ih = h->info_hashes();
  auto hash = ih.has_v2() ? ih.v2.to_string() : ih.v1.to_string();
  lt::aux::to_hex(hash, out);
  out[40] = '\0';
  return 0;
}
