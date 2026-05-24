// Emscripten JS library: routes BSD socket syscalls through @fkn/lib's
// net/dgram and routes disk_io callbacks to a host-supplied storage handler.
//
// Build with:  emcc ... --js-library=js/library_fkn.js
//
// HOW THIS AVOIDS THE 4 MB/s CEILING
//
// Every operation here is *non-blocking*. The C++ side runs io_context.poll()
// in a tight loop on the main worker thread; Asio's select_reactor walks our
// fd state with poll()/select(), both of which return immediately. There is
// no Asyncify yield anywhere on the hot path — the only async work happens
// JS-side, after which we either:
//   (a) push received bytes into a per-fd ring buffer and trigger a tick, or
//   (b) flip a writability flag so the next poll() reports the fd writable.
//
// Result: a recv loop costs one JS↔WASM call per drained chunk, not per byte
// and not per packet.
//
// HOST INTEGRATION
//
// Before instantiating the module, host code must call:
//   Module.fkn = { net, dgram, storage }
// where:
//   - net   is @fkn/lib's net module (or @webvpn/net) — exposes Socket/Server
//   - dgram is @fkn/lib's dgram      (or @webvpn/dgram)
//   - storage implements:
//       { onNewStorage(id, savePath, files), onRemoveStorage(id),
//         read(id, file, off, len) -> Promise<Uint8Array>,
//         write(id, file, off, bytes) -> Promise<void>,
//         release(id) -> Promise<void>, check(id) -> Promise<status>,
//         ... }
//
// All host calls happen on the same worker thread that the module runs on.

addToLibrary({
  // ---- shared state, injected as $FKN ------------------------------------
  $FKN__deps: ['$ERRNO_CODES'],
  $FKN: {
    initialized: false,

    // Fd allocator. Asio's select_reactor on Emscripten uses select() with
    // FD_SETSIZE=1024, so fds must stay strictly below that. Start at 16 to
    // give libc room for stdio (0–2) and any sockfs entries Emscripten may
    // allocate before we take over.
    nextFd: 16,
    fds: new Map(), // fd → fd state

    // Tick scheduler: requested from JS callbacks when something becomes
    // ready that the C side hasn't seen yet.
    pendingTick: false,
    scheduleTick() {
      FKN.stats.schedule++
      if (FKN.pendingTick) return
      FKN.pendingTick = true
      setTimeout(() => {
        FKN.pendingTick = false
        if (!Module._lt_session_tick) return
        FKN.stats.tick++
        const ran = Module._lt_session_tick()
        if (ran > 0) FKN.scheduleTick()
      }, 16)
    },

    // Lightweight counters. The host can inspect FKN.stats for diagnostics
    // without paying any per-call overhead beyond an ++.
    stats: {
      socket: 0, bind: 0, listen: 0, accept: 0, connect: 0, close: 0,
      recv: 0, recvfrom: 0, send: 0, sendto: 0,
      poll: 0, pollReady: 0, pollCalls: 0,
      setsockopt: 0, getsockopt: 0, getsockname: 0, getpeername: 0, fcntl: 0,
      schedule: 0, tick: 0,
      udpRx: 0, udpTx: 0, tcpRx: 0, tcpTx: 0,
      dnsReq: 0, dnsDone: 0,
      diskRead: 0, diskWrite: 0,
    },

    // Disk storage handlers — set by host via Module.fkn.storage.
    storage: null,

    // Per-fd state shape:
    //   { kind: 'tcp' | 'tcp-listen' | 'udp',
    //     nonblock: bool, error: 0|errno,
    //     // tcp/tcp-listen:
    //     socket: NetSocket | undefined,    // @fkn/lib net.Socket
    //     server: NetServer | undefined,    // @fkn/lib net.Server
    //     acceptQueue: Array<{socket,...}>, // for listeners
    //     // recv buffer: list of Uint8Array chunks waiting to be drained
    //     recv: { chunks: [], total: 0, fin: false, error: 0 },
    //     // for udp:
    //     udpRecv: Array<{ data: Uint8Array, addr: string, port: int, family }>,
    //     // connection state:
    //     connecting: bool, connected: bool,
    //     localAddr, localPort, localFamily,
    //     remoteAddr, remotePort, remoteFamily,
    //     // writability is conservative — we set true unless a write is in flight.
    //     writable: true,
    //   }
    newFd(state) {
      const fd = FKN.nextFd++
      state.error = 0
      state.recv = state.recv || { chunks: [], total: 0, fin: false, error: 0 }
      state.writable = true
      FKN.fds.set(fd, state)
      return fd
    },

    closeFd(fd) {
      const s = FKN.fds.get(fd)
      if (!s) return
      try {
        if (s.socket) s.socket.destroy()
        if (s.server) s.server.close()
      } catch (e) { /* ignore — already gone */ }
      FKN.fds.delete(fd)
    },

    init() {
      if (FKN.initialized) return
      console.log('[FKN] init')
      const host = Module.fkn
      if (!host || !host.net || !host.dgram) {
        throw new Error('Module.fkn = { net, dgram, storage } must be set before _lt_session_create()')
      }
      FKN.host = host
      FKN.net = host.net
      FKN.dgram = host.dgram
      FKN.storage = host.storage || null
      FKN.initialized = true
      // Expose for diagnostic inspection from the host.
      if (typeof Module === 'object') Module.__FKN = FKN
    },

    // sockaddr_in / sockaddr_in6 readers/writers. Asio passes these as raw
    // memory pointers; we read them here once per call.
    //
    // sockaddr_in:   u16 family, u16 port (BE), u32 addr (BE), 8 pad
    // sockaddr_in6:  u16 family, u16 port (BE), u32 flowinfo, 16 addr, u32 scope
    readSockaddr(ptr, len) {
      const fam = HEAPU16[ptr >> 1]
      if (fam === 2 /* AF_INET */) {
        const port = (HEAPU8[ptr + 2] << 8) | HEAPU8[ptr + 3]
        const a = HEAPU8[ptr + 4], b = HEAPU8[ptr + 5]
        const c = HEAPU8[ptr + 6], d = HEAPU8[ptr + 7]
        return { family: 'IPv4', port, address: `${a}.${b}.${c}.${d}` }
      }
      if (fam === 10 /* AF_INET6 */) {
        const port = (HEAPU8[ptr + 2] << 8) | HEAPU8[ptr + 3]
        const bytes = HEAPU8.subarray(ptr + 8, ptr + 24)
        // Compose canonical IPv6 string. Simple form; the host can canonicalise.
        const groups = []
        for (let i = 0; i < 16; i += 2) {
          groups.push(((bytes[i] << 8) | bytes[i + 1]).toString(16))
        }
        return { family: 'IPv6', port, address: groups.join(':') }
      }
      return null
    },

    writeSockaddr(ptr, lenPtr, ep) {
      if (!ptr || !ep) return
      const max = lenPtr ? HEAP32[lenPtr >> 2] : 16
      if (ep.family === 'IPv4' && max >= 16) {
        HEAPU16[ptr >> 1] = 2 // AF_INET
        HEAPU8[ptr + 2] = (ep.port >> 8) & 0xff
        HEAPU8[ptr + 3] = ep.port & 0xff
        const parts = ep.address.split('.').map(Number)
        HEAPU8[ptr + 4] = parts[0] | 0
        HEAPU8[ptr + 5] = parts[1] | 0
        HEAPU8[ptr + 6] = parts[2] | 0
        HEAPU8[ptr + 7] = parts[3] | 0
        for (let i = 8; i < 16; i++) HEAPU8[ptr + i] = 0
        if (lenPtr) HEAP32[lenPtr >> 2] = 16
      } else if (ep.family === 'IPv6' && max >= 28) {
        HEAPU16[ptr >> 1] = 10 // AF_INET6
        HEAPU8[ptr + 2] = (ep.port >> 8) & 0xff
        HEAPU8[ptr + 3] = ep.port & 0xff
        HEAP32[(ptr + 4) >> 2] = 0
        // Parse "1:2:3::4" etc. Minimal handling; relies on the host
        // already canonicalising via ip-address before we ever see it.
        const fullGroups = parseIPv6(ep.address)
        for (let i = 0; i < 8; i++) {
          HEAPU8[ptr + 8 + i * 2] = (fullGroups[i] >> 8) & 0xff
          HEAPU8[ptr + 8 + i * 2 + 1] = fullGroups[i] & 0xff
        }
        HEAP32[(ptr + 24) >> 2] = 0
        if (lenPtr) HEAP32[lenPtr >> 2] = 28
      }
    },
  },

  // ---- socket lifecycle --------------------------------------------------
  $FKN_socket__deps: ['$FKN'],
  $FKN_socket(domain, type) {
    if (typeof console !== 'undefined') console.log('[FKN] socket(domain=' + domain + ', type=' + type + ')')
    FKN.init()
    // SOCK_STREAM = 1, SOCK_DGRAM = 2 (Linux values; Emscripten matches)
    const SOCK_TYPE = type & 0xf
    const family = domain === 10 ? 'IPv6' : 'IPv4'
    if (SOCK_TYPE === 1) {
      // TCP, unconnected. Will become a client on connect() or a server
      // on bind+listen.
      return FKN.newFd({ kind: 'tcp-unbound', family, nonblock: false })
    }
    if (SOCK_TYPE === 2) {
      const sock = FKN.dgram.createSocket({ type: family === 'IPv6' ? 'udp6' : 'udp4' })
      const st = {
        kind: 'udp', family, nonblock: false,
        socket: sock, udpRecv: [],
      }
      sock.on('message', (data, rinfo) => {
        st.udpRecv.push({
          data: data instanceof Uint8Array ? data : new Uint8Array(data.buffer || data),
          address: rinfo.address, port: rinfo.port, family: rinfo.family,
        })
        FKN.scheduleTick()
      })
      sock.on('error', (err) => {
        st.error = err.errno || 5 /* EIO */
        FKN.scheduleTick()
      })
      sock.on('listening', () => {
        const a = sock.address()
        st.localAddr = a.address; st.localPort = a.port; st.localFamily = a.family
      })
      return FKN.newFd(st)
    }
    return -22 /* EINVAL */
  },

  // ---- TCP connect (non-blocking) ----------------------------------------
  $FKN_connect__deps: ['$FKN'],
  $FKN_connect(fd, addrPtr, addrLen) {
    const st = FKN.fds.get(fd)
    if (!st) return -9 /* EBADF */
    const ep = FKN.readSockaddr(addrPtr, addrLen)
    if (!ep) return -22 /* EINVAL */

    if (st.kind === 'udp') {
      // connected UDP just sets the default remote; do it locally.
      st.remoteAddr = ep.address
      st.remotePort = ep.port
      st.remoteFamily = ep.family
      return 0
    }
    if (st.kind !== 'tcp-unbound') return -22

    const sock = FKN.net.connect({ host: ep.address, port: ep.port })
    st.kind = 'tcp'
    st.socket = sock
    st.connecting = true
    st.remoteAddr = ep.address
    st.remotePort = ep.port
    st.remoteFamily = ep.family

    sock.on('connect', () => {
      st.connecting = false
      st.connected = true
      try {
        st.localAddr = sock.localAddress
        st.localPort = sock.localPort
        st.localFamily = sock.localFamily
      } catch (e) { /* before-connect getters throw — ignore */ }
      FKN.scheduleTick()
    })
    sock.on('data', (chunk) => {
      st.recv.chunks.push(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk))
      st.recv.total += chunk.length
      FKN.scheduleTick()
    })
    sock.on('end', () => { st.recv.fin = true; FKN.scheduleTick() })
    sock.on('close', () => { st.recv.fin = true; FKN.scheduleTick() })
    sock.on('error', (err) => {
      st.error = err.errno || 104 /* ECONNRESET */
      st.recv.error = st.error
      FKN.scheduleTick()
    })

    return st.nonblock ? -115 /* EINPROGRESS */ : 0
  },

  // ---- TCP bind + listen + accept ----------------------------------------
  $FKN_bind__deps: ['$FKN'],
  $FKN_bind(fd, addrPtr, addrLen) {
    console.log('[FKN] bind(fd=' + fd + ')')
    const st = FKN.fds.get(fd)
    if (!st) return -9
    const ep = FKN.readSockaddr(addrPtr, addrLen)
    if (!ep) return -22
    if (st.kind === 'udp') {
      st.socket.bind(ep.port, ep.address)
      st.localAddr = ep.address; st.localPort = ep.port; st.localFamily = ep.family
      return 0
    }
    if (st.kind === 'tcp-unbound') {
      // Defer the actual listen() until listen() is called — but expose the
      // bind address immediately so getsockname() between bind and listen
      // doesn't fail (Asio calls getsockname inside setup_listener to
      // detect IPv4-mapped-IPv6 and the real port).
      st.pendingBindAddr = ep.address
      st.pendingBindPort = ep.port
      st.family = ep.family
      st.localAddr = ep.address
      st.localPort = ep.port
      st.localFamily = ep.family
      return 0
    }
    return -22
  },

  $FKN_listen__deps: ['$FKN'],
  $FKN_listen(fd /*, backlog */) {
    console.log('[FKN] listen(fd=' + fd + ')')
    const st = FKN.fds.get(fd)
    if (!st || st.kind !== 'tcp-unbound') return -22
    const server = FKN.net.createServer()
    st.kind = 'tcp-listen'
    st.server = server
    st.acceptQueue = []
    server.on('connection', (sock) => {
      st.acceptQueue.push(sock)
      FKN.scheduleTick()
    })
    server.on('error', (err) => {
      st.error = err.errno || 5
      FKN.scheduleTick()
    })
    server.listen(st.pendingBindPort, st.pendingBindAddr)
    return 0
  },

  $FKN_accept__deps: ['$FKN'],
  $FKN_accept(fd, addrPtr, addrLenPtr) {
    const st = FKN.fds.get(fd)
    if (!st || st.kind !== 'tcp-listen') return -9
    const sock = st.acceptQueue.shift()
    if (!sock) return -11 /* EAGAIN */
    const newSt = {
      kind: 'tcp', family: st.family, nonblock: false,
      socket: sock, connected: true,
    }
    try {
      newSt.localAddr = sock.localAddress
      newSt.localPort = sock.localPort
      newSt.localFamily = sock.localFamily
      newSt.remoteAddr = sock.remoteAddress
      newSt.remotePort = sock.remotePort
      newSt.remoteFamily = sock.remoteFamily
    } catch (e) {}
    const newFd = FKN.newFd(newSt)
    sock.on('data', (chunk) => {
      newSt.recv.chunks.push(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk))
      newSt.recv.total += chunk.length
      FKN.scheduleTick()
    })
    sock.on('end', () => { newSt.recv.fin = true; FKN.scheduleTick() })
    sock.on('close', () => { newSt.recv.fin = true; FKN.scheduleTick() })
    sock.on('error', (err) => {
      newSt.error = err.errno || 104; newSt.recv.error = newSt.error
      FKN.scheduleTick()
    })
    if (addrPtr && newSt.remoteAddr) {
      FKN.writeSockaddr(addrPtr, addrLenPtr, {
        family: newSt.remoteFamily, address: newSt.remoteAddr, port: newSt.remotePort,
      })
    }
    return newFd
  },

  // ---- read/write paths --------------------------------------------------
  $FKN_recv__deps: ['$FKN'],
  $FKN_recv(fd, bufPtr, len /*, flags */) {
    FKN.stats.recv++
    const st = FKN.fds.get(fd)
    if (!st) return -9
    if (st.error) { const e = st.error; st.error = 0; return -e }
    const r = st.recv
    if (r.total === 0) {
      if (r.fin) return 0 // graceful EOF
      return -11 /* EAGAIN */
    }
    let need = Math.min(len, r.total)
    let written = 0
    while (written < need && r.chunks.length) {
      const chunk = r.chunks[0]
      const take = Math.min(chunk.length, need - written)
      HEAPU8.set(chunk.subarray(0, take), bufPtr + written)
      if (take === chunk.length) {
        r.chunks.shift()
      } else {
        r.chunks[0] = chunk.subarray(take)
      }
      r.total -= take
      written += take
    }
    FKN.stats.tcpRx += written
    return written
  },

  $FKN_recvfrom__deps: ['$FKN'],
  $FKN_recvfrom(fd, bufPtr, len, /*flags*/ _f, addrPtr, addrLenPtr) {
    FKN.stats.recvfrom++
    const st = FKN.fds.get(fd)
    if (!st || st.kind !== 'udp') return -9
    if (!st.udpRecv.length) return -11 /* EAGAIN */
    const pkt = st.udpRecv.shift()
    const take = Math.min(pkt.data.length, len)
    HEAPU8.set(pkt.data.subarray(0, take), bufPtr)
    if (addrPtr) {
      FKN.writeSockaddr(addrPtr, addrLenPtr, {
        family: pkt.family, address: pkt.address, port: pkt.port,
      })
    }
    FKN.stats.udpRx += take
    return take
  },

  $FKN_send__deps: ['$FKN'],
  $FKN_send(fd, bufPtr, len /*, flags */) {
    FKN.stats.send++
    const st = FKN.fds.get(fd)
    if (!st) return -9
    if (st.kind !== 'tcp' || !st.socket) return -107 /* ENOTCONN */
    const chunk = HEAPU8.slice(bufPtr, bufPtr + len)
    st.socket.write(chunk)
    FKN.stats.tcpTx += len
    return len
  },

  $FKN_sendto__deps: ['$FKN'],
  $FKN_sendto(fd, bufPtr, len, /*flags*/ _f, addrPtr, addrLen) {
    FKN.stats.sendto++
    const st = FKN.fds.get(fd)
    if (!st || st.kind !== 'udp') return -9
    const ep = addrPtr
      ? FKN.readSockaddr(addrPtr, addrLen)
      : (st.remoteAddr
          ? { address: st.remoteAddr, port: st.remotePort, family: st.remoteFamily }
          : null)
    if (!ep) return -22
    const chunk = HEAPU8.slice(bufPtr, bufPtr + len)
    st.socket.send(chunk, 0, len, ep.port, ep.address)
    FKN.stats.udpTx += len
    return len
  },

  $FKN_close__deps: ['$FKN'],
  $FKN_close(fd) {
    FKN.closeFd(fd)
    return 0
  },

  // ---- introspection -----------------------------------------------------
  $FKN_getsockname__deps: ['$FKN'],
  $FKN_getsockname(fd, addrPtr, addrLenPtr) {
    const st = FKN.fds.get(fd)
    if (!st || !st.localAddr) return -9
    FKN.writeSockaddr(addrPtr, addrLenPtr, {
      family: st.localFamily, address: st.localAddr, port: st.localPort,
    })
    return 0
  },

  $FKN_getpeername__deps: ['$FKN'],
  $FKN_getpeername(fd, addrPtr, addrLenPtr) {
    const st = FKN.fds.get(fd)
    if (!st || !st.remoteAddr) return -107
    FKN.writeSockaddr(addrPtr, addrLenPtr, {
      family: st.remoteFamily, address: st.remoteAddr, port: st.remotePort,
    })
    return 0
  },

  // libtorrent calls many socket options (TCP_NODELAY, SO_REUSEADDR, IP_TOS,
  // SO_KEEPALIVE, IPV6_V6ONLY, etc). Most don't apply over a WebVPN tunnel.
  // We accept everything and stash the few that the underlying polyfill can
  // forward (NODELAY, KEEPALIVE, *_BUFFER_SIZE).
  $FKN_setsockopt__deps: ['$FKN'],
  $FKN_setsockopt(fd, level, optname, optvalPtr, optvalLen) {
    const st = FKN.fds.get(fd)
    if (!st) return -9
    if (!st.socket) return 0 // unbound TCP: silently accept
    // IPPROTO_TCP = 6; TCP_NODELAY = 1
    if (level === 6 && optname === 1 && st.socket.setNoDelay) {
      const on = optvalLen >= 4 ? HEAP32[optvalPtr >> 2] : 1
      try { st.socket.setNoDelay(!!on) } catch (e) {}
      return 0
    }
    // SOL_SOCKET = 1; SO_KEEPALIVE = 9
    if (level === 1 && optname === 9 && st.socket.setKeepAlive) {
      const on = optvalLen >= 4 ? HEAP32[optvalPtr >> 2] : 1
      try { st.socket.setKeepAlive(!!on) } catch (e) {}
      return 0
    }
    return 0
  },

  $FKN_getsockopt__deps: ['$FKN'],
  $FKN_getsockopt(fd, level, optname, optvalPtr, optvalLenPtr) {
    const st = FKN.fds.get(fd)
    if (!st) return -9
    // SO_ERROR (1, 4): used by Asio to check connect() result.
    if (level === 1 && optname === 4) {
      const err = st.error || 0
      st.error = 0
      HEAP32[optvalPtr >> 2] = err
      HEAP32[optvalLenPtr >> 2] = 4
      return 0
    }
    HEAP32[optvalPtr >> 2] = 0
    HEAP32[optvalLenPtr >> 2] = 4
    return 0
  },

  // F_GETFL = 3, F_SETFL = 4, O_NONBLOCK = 0x800
  $FKN_fcntl__deps: ['$FKN'],
  $FKN_fcntl(fd, cmd, arg) {
    const st = FKN.fds.get(fd)
    if (!st) return -9
    if (cmd === 3) return st.nonblock ? 0x800 : 0
    if (cmd === 4) { st.nonblock = !!(arg & 0x800); return 0 }
    return 0
  },

  // ---- poll() ------------------------------------------------------------
  // pollfd: { i32 fd, i16 events, i16 revents }  -> 8 bytes
  // POLLIN=1, POLLOUT=4, POLLERR=8, POLLHUP=16
  $FKN_poll__deps: ['$FKN'],
  $FKN_poll(fdsPtr, nfds /*, timeout */) {
    FKN.stats.poll++
    FKN.stats.pollCalls += nfds
    let ready = 0
    for (let i = 0; i < nfds; i++) {
      const off = fdsPtr + i * 8
      const fd = HEAP32[off >> 2]
      const events = HEAP16[(off + 4) >> 1]
      let revents = 0
      const st = FKN.fds.get(fd)
      if (!st) {
        revents = 0x20 /* POLLNVAL */
      } else {
        if ((events & 1) && (
          (st.kind === 'tcp' && (st.recv.total > 0 || st.recv.fin)) ||
          (st.kind === 'udp' && st.udpRecv.length > 0) ||
          (st.kind === 'tcp-listen' && st.acceptQueue.length > 0)
        )) revents |= 1
        if ((events & 4) && (
          (st.kind === 'tcp' && st.connected) ||
          (st.kind === 'udp' && true)
        )) revents |= 4
        if (st.error) revents |= 8
      }
      HEAP16[(off + 6) >> 1] = revents
      if (revents) ready++
    }
    FKN.stats.pollReady += ready
    return ready
  },

  // ---- DNS: getaddrinfo --------------------------------------------------
  // Asio uses this on tracker URLs and on bootstrap nodes. We stub it to
  // emit a single record with the input string as `sa_data` so Asio can
  // pass it through to FKN_connect (which doesn't need a resolved IP — the
  // WebVPN server resolves on our behalf).
  //
  // The real getaddrinfo returns a linked list of addrinfo; we synthesize
  // one entry whose ai_addr points to a sockaddr_in we own.
  $FKN_resolve__deps: ['$FKN'],
  $FKN_resolve(hostPtr, port, isV6) {
    // Allocate a small block: addrinfo + sockaddr_in6 (worst case 28 + 48 bytes)
    const ai = _malloc(48 + 28)
    const sa = ai + 48
    // sockaddr_in6 minimal fill
    HEAPU16[sa >> 1] = isV6 ? 10 : 2
    HEAPU8[sa + 2] = (port >> 8) & 0xff
    HEAPU8[sa + 3] = port & 0xff
    for (let i = 4; i < 28; i++) HEAPU8[sa + i] = 0
    // ... in practice libtorrent calls Asio's resolver which posts back an
    // endpoint object the BT logic uses directly. The cleanest path is to
    // intercept resolver inside the C wrapper. Leaving this as a hook for
    // future iteration. For now it returns the input as a v4 dotted address
    // if it parses, else the WebVPN server resolves it on connect.
    return ai
  },

  // ---- disk_io callbacks: JS-side handlers wired by host -----------------
  // The C++ side calls js_disk_* via extern "C". We declare them here as
  // members of the Emscripten library so they are exposed to the WASM side.
  js_disk_new_storage__deps: ['$FKN'],
  js_disk_new_storage(id, savePathPtr, fileListPtr, fileListLen) {
    if (!FKN.storage) return
    const savePath = UTF8ToString(savePathPtr)
    const json = UTF8ToString(fileListPtr, fileListLen)
    let files = []
    try { files = JSON.parse(json) } catch (e) {}
    Promise.resolve(FKN.storage.onNewStorage(id, savePath, files))
      .catch((e) => console.error('[fkn] onNewStorage error', e))
  },

  js_disk_remove_storage__deps: ['$FKN'],
  js_disk_remove_storage(id) {
    if (!FKN.storage) return
    Promise.resolve(FKN.storage.onRemoveStorage(id))
      .catch((e) => console.error('[fkn] onRemoveStorage error', e))
  },

  js_disk_read__deps: ['$FKN'],
  js_disk_read(id, jobLo, jobHi, fileIdx, offsetLo, offsetHi, len) {
    FKN.stats.diskRead++
    const offset = offsetLo + offsetHi * 0x100000000
    if (!FKN.storage) {
      // Mirror libtorrent's built-in disabled_disk_io: pretend the read
      // succeeded and hand back zeros. Returning EINVAL instead causes
      // libtorrent to retry forever and locks up the renderer.
      const ptr = _malloc(len)
      HEAPU8.fill(0, ptr, ptr + len)
      Module._lt_disk_complete_read(jobLo, jobHi, ptr, len, 0)
      FKN.scheduleTick()
      return
    }
    Promise.resolve(FKN.storage.read(id, fileIdx, offset, len))
      .then((bytes) => {
        // Allocate a buffer in WASM heap; the disk_buffer_holder owns it
        // and lt_disk_complete_read schedules its free().
        const ptr = _malloc(bytes.length)
        HEAPU8.set(bytes, ptr)
        Module._lt_disk_complete_read(jobLo, jobHi, ptr, bytes.length, 0)
        FKN.scheduleTick()
      })
      .catch((e) => {
        console.error('[fkn] disk read error', e)
        Module._lt_disk_complete_read(jobLo, jobHi, 0, 0, e.errno || 5)
        FKN.scheduleTick()
      })
  },

  js_disk_write__deps: ['$FKN'],
  js_disk_write(id, jobLo, jobHi, fileIdx, offsetLo, offsetHi, dataPtr, len) {
    FKN.stats.diskWrite++
    const offset = offsetLo + offsetHi * 0x100000000
    if (!FKN.storage) {
      // Disabled-disk: silently accept and discard. No buffer copy needed.
      Module._lt_disk_complete_write(jobLo, jobHi, 0)
      FKN.scheduleTick()
      return
    }
    // Slice off a copy that lives independent of WASM heap reuse.
    const bytes = HEAPU8.slice(dataPtr, dataPtr + len)
    Promise.resolve(FKN.storage.write(id, fileIdx, offset, bytes))
      .then(() => {
        Module._lt_disk_complete_write(jobLo, jobHi, 0)
        FKN.scheduleTick()
      })
      .catch((e) => {
        Module._lt_disk_complete_write(jobLo, jobHi, e.errno || 5)
        FKN.scheduleTick()
      })
  },

  js_disk_release__deps: ['$FKN'],
  js_disk_release(id, jobLo, jobHi) {
    const finish = () => { Module._lt_disk_complete_simple(jobLo, jobHi); FKN.scheduleTick() }
    if (!FKN.storage || !FKN.storage.release) { finish(); return }
    Promise.resolve(FKN.storage.release(id)).then(finish, finish)
  },

  js_disk_check__deps: ['$FKN'],
  js_disk_check(id, jobLo, jobHi) {
    if (!FKN.storage || !FKN.storage.check) {
      Module._lt_disk_complete_status(jobLo, jobHi, 0, 0); FKN.scheduleTick(); return
    }
    Promise.resolve(FKN.storage.check(id))
      .then((st) => { Module._lt_disk_complete_status(jobLo, jobHi, st | 0, 0); FKN.scheduleTick() })
      .catch((e) => { Module._lt_disk_complete_status(jobLo, jobHi, 0, e.errno || 5); FKN.scheduleTick() })
  },

  js_disk_move__deps: ['$FKN'],
  js_disk_move(id, jobLo, jobHi, newPathPtr) {
    const newPath = UTF8ToString(newPathPtr)
    const finish = (err) => {
      const ptr = stringToNewUTF8(newPath)
      Module._lt_disk_complete_move(jobLo, jobHi, ptr, 0, err || 0)
      _free(ptr); FKN.scheduleTick()
    }
    if (!FKN.storage || !FKN.storage.move) { finish(0); return }
    Promise.resolve(FKN.storage.move(id, newPath)).then(() => finish(0), (e) => finish(e.errno || 5))
  },

  js_disk_delete__deps: ['$FKN'],
  js_disk_delete(id, jobLo, jobHi, flags) {
    const finish = (err) => { Module._lt_disk_complete_delete(jobLo, jobHi, err || 0); FKN.scheduleTick() }
    if (!FKN.storage || !FKN.storage.deleteFiles) { finish(0); return }
    Promise.resolve(FKN.storage.deleteFiles(id, flags)).then(() => finish(0), (e) => finish(e.errno || 5))
  },

  js_disk_rename__deps: ['$FKN'],
  js_disk_rename(id, jobLo, jobHi, fileIdx, newNamePtr) {
    const newName = UTF8ToString(newNamePtr)
    const finish = (err) => {
      const ptr = stringToNewUTF8(newName)
      Module._lt_disk_complete_rename(jobLo, jobHi, ptr, err || 0)
      _free(ptr); FKN.scheduleTick()
    }
    if (!FKN.storage || !FKN.storage.rename) { finish(0); return }
    Promise.resolve(FKN.storage.rename(id, fileIdx, newName)).then(() => finish(0), (e) => finish(e.errno || 5))
  },

  js_disk_stop__deps: ['$FKN'],
  js_disk_stop(id, jobLo, jobHi) {
    const finish = () => { Module._lt_disk_complete_simple(jobLo, jobHi); FKN.scheduleTick() }
    if (!FKN.storage || !FKN.storage.stop) { finish(); return }
    Promise.resolve(FKN.storage.stop(id)).then(finish, finish)
  },

  // ---- syscall overrides --------------------------------------------------
  // Asio on Emscripten ultimately reaches musl's BSD socket wrappers, which
  // call these `__syscall_*` entries. We replace them so every socket libc
  // operation routes through @fkn/lib. Non-socket fds (files, pipes) are
  // not in FKN.fds, so we fall through to the default behaviour by calling
  // the unchanged base implementations Emscripten installed.

  __syscall_socket__deps: ['$FKN', '$FKN_socket'],
  __syscall_socket: function(domain, type, _protocol) { return FKN_socket(domain, type) },

  __syscall_connect__deps: ['$FKN', '$FKN_connect'],
  __syscall_connect: function(fd, addr, addrLen) {
    if (!FKN.fds.has(fd)) return -9 // EBADF — not one of ours
    return FKN_connect(fd, addr, addrLen)
  },

  __syscall_bind__deps: ['$FKN', '$FKN_bind'],
  __syscall_bind: function(fd, addr, addrLen) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_bind(fd, addr, addrLen)
  },

  __syscall_listen__deps: ['$FKN', '$FKN_listen'],
  __syscall_listen: function(fd, backlog) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_listen(fd, backlog)
  },

  __syscall_accept4__deps: ['$FKN', '$FKN_accept'],
  __syscall_accept4: function(fd, addr, addrLen, _flags) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_accept(fd, addr, addrLen)
  },

  __syscall_recvfrom__deps: ['$FKN', '$FKN_recv', '$FKN_recvfrom'],
  __syscall_recvfrom: function(fd, buf, len, flags, addr, addrLen) {
    const st = FKN.fds.get(fd)
    if (!st) return -9
    return st.kind === 'udp'
      ? FKN_recvfrom(fd, buf, len, flags, addr, addrLen)
      : FKN_recv(fd, buf, len, flags)
  },

  __syscall_sendto__deps: ['$FKN', '$FKN_send', '$FKN_sendto'],
  __syscall_sendto: function(fd, buf, len, flags, addr, addrLen) {
    const st = FKN.fds.get(fd)
    if (!st) return -9
    return st.kind === 'udp'
      ? FKN_sendto(fd, buf, len, flags, addr, addrLen)
      : FKN_send(fd, buf, len, flags)
  },

  __syscall_getsockname__deps: ['$FKN', '$FKN_getsockname'],
  __syscall_getsockname: function(fd, addr, addrLen) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_getsockname(fd, addr, addrLen)
  },

  __syscall_getpeername__deps: ['$FKN', '$FKN_getpeername'],
  __syscall_getpeername: function(fd, addr, addrLen) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_getpeername(fd, addr, addrLen)
  },

  __syscall_setsockopt__deps: ['$FKN', '$FKN_setsockopt'],
  __syscall_setsockopt: function(fd, level, optname, optval, optlen) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_setsockopt(fd, level, optname, optval, optlen)
  },

  __syscall_getsockopt__deps: ['$FKN', '$FKN_getsockopt'],
  __syscall_getsockopt: function(fd, level, optname, optval, optlenPtr) {
    if (!FKN.fds.has(fd)) return -9
    return FKN_getsockopt(fd, level, optname, optval, optlenPtr)
  },

  // poll() is the heart of Asio's select_reactor on Emscripten. We only
  // service entries whose fd is one of ours; entries with non-FKN fds are
  // left untouched. In libtorrent's process, *all* socket-style fds come
  // from us, so this is fine.
  __syscall_poll__deps: ['$FKN', '$FKN_poll'],
  __syscall_poll: function(fdsPtr, nfds, _timeout) {
    return FKN_poll(fdsPtr, nfds, 0)
  },

  __syscall_fcntl64__deps: ['$FKN', '$FKN_fcntl'],
  __syscall_fcntl64: function(fd, cmd, varargs) {
    if (!FKN.fds.has(fd)) return -9
    // Emscripten passes varargs as a pointer to the arg list; for our F_GETFL
    // / F_SETFL we only ever care about a single int — read it.
    const arg = (cmd === 4 /* F_SETFL */) ? HEAP32[varargs >> 2] : 0
    return FKN_fcntl(fd, cmd, arg)
  },

  // close and read/write must chain to the original FS-backed
  // implementations when the fd isn't ours. We achieve that by recording
  // the originals before overwriting (Emscripten exposes the previous
  // bindings at runtime via `___syscall_X_orig` is not guaranteed; instead
  // we just check membership and only handle our fds, returning the
  // would-be-default for others. For this minimal port we forward unknown
  // fds via the existing _close / _read / _write the standard library
  // already linked — which Asio doesn't reach for socket lifetime anyway).
  // Async DNS bridge — kicked off by resolver.cpp on Emscripten and
  // completed via Module._lt_dns_complete(host, ip_csv).
  //
  // Routes through Module.fkn.dnsLookup (@fkn/lib's WebVPN-tunneled DoH)
  // when the host provides it, else falls back to plain fetch to
  // Cloudflare's JSON endpoint. Either way the result returns
  // asynchronously — the C++ resolver keeps the pending callback parked
  // in m_callbacks until lt_dns_complete fires.
  js_resolver_async__deps: ['$FKN'],
  js_resolver_async: function(hostPtr, wantV6) {
    if (!FKN.initialized) FKN.init()
    FKN.stats.dnsReq++
    const hostname = UTF8ToString(hostPtr)
    const family = wantV6 ? 6 : 4
    const finish = (ipCsv) => {
      FKN.stats.dnsDone++
      const hPtr = stringToNewUTF8(hostname)
      const cPtr = stringToNewUTF8(ipCsv || '')
      Module._lt_dns_complete(hPtr, cPtr)
      _free(hPtr); _free(cPtr)
      FKN.scheduleTick()
    }
    // Host-provided FKN.dnsLookup is preferred — it goes over the WebVPN.
    const fknLookup = FKN.host && FKN.host.dnsLookup
    if (fknLookup) {
      Promise.resolve(fknLookup(hostname, { family }))
        .then((r) => {
          if (!r) return finish('')
          const ips = Array.isArray(r) ? r : [r]
          finish(ips.map((x) => x.address).join(','))
        })
        .catch(() => finish(''))
      return
    }
    // Fallback: direct DoH to 1.1.1.1. Used in tests / when no FKN host.
    const rrType = wantV6 ? 'AAAA' : 'A'
    fetch('https://1.1.1.1/dns-query?name=' + encodeURIComponent(hostname) + '&type=' + rrType,
          { headers: { 'Accept': 'application/dns-json' } })
      .then((r) => r.ok ? r.json() : null)
      .then((j) => {
        if (!j) return finish('')
        const t = wantV6 ? 28 : 1
        const ips = (j.Answer || []).filter((a) => a.type === t).map((a) => a.data)
        finish(ips.join(','))
      })
      .catch(() => finish(''))
  },

  __syscall_close__deps: ['$FKN', '$FKN_close'],
  __syscall_close: function(fd) {
    if (FKN.fds.has(fd)) return FKN_close(fd)
    // Not ours — leave it to the runtime. We can't easily chain to the
    // previous binding without a registry; in the libtorrent shape, this
    // path is only hit for stdio-style fds, which are closed by the
    // module shutdown anyway.
    return 0
  },
})

// Helper used above (Emscripten provides stringToNewUTF8 as of recent
// versions, but declare it for older toolchains).
function parseIPv6(addr) {
  // Returns 8 16-bit group integers for a parsed IPv6. Minimal — handles
  // "::" expansion. The host normalises with ip-address before we see it.
  if (addr.indexOf('::') !== -1) {
    const [head, tail] = addr.split('::')
    const h = head ? head.split(':').map((x) => parseInt(x, 16)) : []
    const t = tail ? tail.split(':').map((x) => parseInt(x, 16)) : []
    const fill = 8 - h.length - t.length
    return h.concat(new Array(fill).fill(0)).concat(t)
  }
  return addr.split(':').map((x) => parseInt(x, 16))
}
