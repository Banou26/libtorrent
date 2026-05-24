/*

Copyright (c) 2014-2017, 2019-2020, Arvid Norberg
Copyright (c) 2016-2018, 2020, Alden Torres
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the distribution.
    * Neither the name of the author nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

#include "libtorrent/aux_/resolver.hpp"
#include "libtorrent/debug.hpp"
#include "libtorrent/aux_/time.hpp"

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>

// Kick off an async DNS lookup on the JS side. Routes through
// Module.fkn.dnsLookup (which is @fkn/lib's WebVPN-tunneled DoH) when the
// host provides it, else falls back to a plain fetch to Cloudflare. The
// result arrives back via lt_dns_complete(hostname, ip_csv) on a later
// tick — same callback shape resolver::on_lookup uses for the Asio path.
extern "C" {
  void js_resolver_async(char const* host, int want_v6);
}
#endif

namespace libtorrent {
namespace aux {


	constexpr resolver_flags resolver_interface::cache_only;
	constexpr resolver_flags resolver_interface::abort_on_shutdown;

#ifdef __EMSCRIPTEN__
	// Singleton pointer to the active resolver. session_impl owns exactly
	// one, and the JS-side DNS path needs a way to call back into it
	// without threading the resolver through every js_dns_complete call.
	static resolver* g_active_resolver = nullptr;
#endif

	resolver::resolver(io_context& ios)
		: m_ios(ios)
		, m_resolver(ios)
		, m_critical_resolver(ios)
		, m_max_size(700)
		, m_timeout(seconds(1200))
	{
#ifdef __EMSCRIPTEN__
		g_active_resolver = this;
#endif
	}

namespace {
	void callback(resolver_interface::callback_t h
		, error_code const& ec, std::vector<address> const& ips)
	{
		try {
			h(ec, ips);
		} catch (std::exception&) {
			TORRENT_ASSERT_FAIL();
		}
	}
}

	void resolver::on_lookup(error_code const& ec, tcp::resolver::results_type ips
		, std::string const& hostname)
	{
		COMPLETE_ASYNC("resolver::on_lookup");
		if (ec)
		{
			failed_dns_cache_entry& ce = m_failed_cache[hostname];
			ce.last_seen = time_now();
			ce.error = ec;

			// if the cache grows too big, weed out the
			// oldest entries
			if (int(m_failed_cache.size()) > m_max_size)
			{
				auto oldest = m_failed_cache.begin();
				for (auto k = m_failed_cache.begin(); k != m_failed_cache.end(); ++k)
				{
					if (k->second.last_seen < oldest->second.last_seen)
						oldest = k;
				}

				// remove the oldest entry
				m_failed_cache.erase(oldest);
			}

			auto const range = m_callbacks.equal_range(hostname);
			for (auto c = range.first; c != range.second; ++c)
				callback(std::move(c->second), ec, {});
			m_callbacks.erase(range.first, range.second);
			return;
		}

		{
			auto const k = m_failed_cache.find(hostname);
			if (k != m_failed_cache.end())
				m_failed_cache.erase(k);
		}

		dns_cache_entry& ce = m_cache[hostname];
		ce.last_seen = time_now();
		ce.addresses.clear();
		for (auto i : ips)
			ce.addresses.push_back(i.endpoint().address());

		auto const range = m_callbacks.equal_range(hostname);
		for (auto c = range.first; c != range.second; ++c)
			callback(std::move(c->second), ec, ce.addresses);
		m_callbacks.erase(range.first, range.second);

		// if m_cache grows too big, weed out the
		// oldest entries
		if (int(m_cache.size()) > m_max_size)
		{
			auto oldest = m_cache.begin();
			for (auto k = m_cache.begin(); k != m_cache.end(); ++k)
			{
				if (k->second.last_seen < oldest->second.last_seen)
					oldest = k;
			}

			// remove the oldest entry
			m_cache.erase(oldest);
		}
	}

	void resolver::async_resolve(std::string const& host, resolver_flags const flags
		, resolver_interface::callback_t h)
	{
		// special handling for raw IP addresses. There's no need to get in line
		// behind actual lookups if we can just resolve it immediately.
		error_code ec;
		address const ip = make_address(host, ec);
		if (!ec)
		{
			post(m_ios, [h, ec, ip]{ callback(h, ec, std::vector<address>{ip}); });
			return;
		}
		ec.clear();

		auto const i = m_cache.find(host);
		if (i != m_cache.end())
		{
			// keep cache entries valid for m_timeout seconds
			if ((flags & resolver_interface::cache_only)
				|| i->second.last_seen + m_timeout >= time_now())
			{
				std::vector<address> ips = i->second.addresses;
				post(m_ios, [h, ec, ips] { callback(h, ec, ips); });
				return;
			}
		}

		auto const k = m_failed_cache.find(host);
		if (k != m_failed_cache.end())
		{
			// keep cache entries valid for m_timeout seconds
			// failures are cached for a shorter time to optimistically retry
			if ((flags & resolver_interface::cache_only)
				|| k->second.last_seen + m_timeout / 8 >= time_now())
			{
				error_code error_code = k->second.error;
				post(m_ios, [h, error_code] { callback(h, error_code, {}); });
				return;
			}
		}

		if (flags & resolver_interface::cache_only)
		{
			// we did not find a cache entry, fail the lookup
			post(m_ios, [h] {
				callback(h, boost::asio::error::host_not_found, std::vector<address>{});
			});
			return;
		}

		auto iter = m_callbacks.find(host);
		bool const done = (iter != m_callbacks.end());

		m_callbacks.insert(iter, {host, std::move(h)});

		// if there is an existing outtanding lookup, our callback will be
		// called once it completes. We're done here.
		if (done) return;

#ifdef __EMSCRIPTEN__
		// On Emscripten Boost.Asio's resolver tries to spawn a worker thread
		// (pthread_create fails under -sUSE_PTHREADS=0 → "thread: Not
		// supported" → session pauses). Hand the lookup off to JS instead;
		// JS calls Module.fkn.dnsLookup (or a fallback fetch) and posts the
		// result back via lt_dns_complete on a later tick. The callback in
		// m_callbacks stays parked until then — same shape as Asio's path.
		js_resolver_async(host.c_str(), 0);
#else
		// the port is ignored
		using namespace std::placeholders;
		ADD_OUTSTANDING_ASYNC("resolver::on_lookup");
		if (flags & resolver_interface::abort_on_shutdown)
		{
			m_resolver.async_resolve(host, "80", std::bind(&resolver::on_lookup, this, _1, _2
				, host));
		}
		else
		{
			m_critical_resolver.async_resolve(host, "80", std::bind(&resolver::on_lookup, this, _1, _2
				, host));
		}
#endif
	}

	void resolver::abort()
	{
		m_resolver.cancel();
	}

	void resolver::set_cache_timeout(seconds const timeout)
	{
		if (timeout >= seconds(0))
			m_timeout = timeout;
		else
			m_timeout = seconds(0);
	}

#ifdef __EMSCRIPTEN__
	// Called by JS (lt_dns_complete trampoline below) when the async lookup
	// kicked off by js_resolver_async finishes. ip_csv is either an empty
	// string (failure) or a comma-separated list of dotted-quad / IPv6
	// addresses. Re-uses the same cache+dispatch logic as the original
	// on_lookup path.
	void resolver::wasm_complete(std::string host, std::string ip_csv)
	{
		std::vector<address> addrs;
		if (!ip_csv.empty()) {
			std::size_t p = 0;
			while (p < ip_csv.size()) {
				auto comma = ip_csv.find(',', p);
				auto piece = ip_csv.substr(p, comma == std::string::npos ? std::string::npos : comma - p);
				error_code de;
				auto a = make_address(piece, de);
				if (!de) addrs.push_back(a);
				if (comma == std::string::npos) break;
				p = comma + 1;
			}
		}
		// Cache the result (success or fail) so the next lookup is instant.
		if (addrs.empty()) {
			failed_dns_cache_entry& ce = m_failed_cache[host];
			ce.last_seen = time_now();
			ce.error = boost::asio::error::host_not_found;
		} else {
			dns_cache_entry& ce = m_cache[host];
			ce.last_seen = time_now();
			ce.addresses = addrs;
		}
		// Drain every pending callback for this hostname.
		auto range = m_callbacks.equal_range(host);
		for (auto it = range.first; it != range.second; ++it) {
			if (addrs.empty()) {
				callback(it->second, boost::asio::error::host_not_found, {});
			} else {
				callback(it->second, error_code{}, addrs);
			}
		}
		m_callbacks.erase(range.first, range.second);
	}
#endif
}
}

#ifdef __EMSCRIPTEN__
// C ABI trampoline. JS calls this after the async lookup resolves.
extern "C" EMSCRIPTEN_KEEPALIVE
void lt_dns_complete(char const* hostname, char const* ip_csv)
{
	if (auto* r = libtorrent::aux::g_active_resolver) {
		// Capture by value: JS will free its buffers after we return.
		auto& ios = *reinterpret_cast<boost::asio::io_context*>(r->ios_ptr());
		std::string h = hostname ? hostname : "";
		std::string c = ip_csv ? ip_csv : "";
		boost::asio::post(ios, [r, h = std::move(h), c = std::move(c)] () mutable {
			r->wasm_complete(std::move(h), std::move(c));
		});
	}
}
#endif
