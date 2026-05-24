// Live integration: real @webvpn/net + dgram via @fkn/lib's iframe to
// http://localhost:1234/api. Bootstraps the WASM session and exposes
// everything on window for manual probing — no periodic intervals so the
// page stays responsive for inspection.

import * as net from '@webvpn/net'
import * as dgram from '@webvpn/dgram'

import factory from './libtorrent.js'

const $ = (id: string) => document.getElementById(id)!
const log = (msg: string, cls = '') => {
  const line = document.createElement('div')
  if (cls) line.className = cls
  line.textContent = msg
  $('alerts').insertBefore(line, $('alerts').firstChild)
  console.log('[live]', msg)
}

window.addEventListener('error', e => log('window error: ' + e.message, 'bad'))
window.addEventListener('unhandledrejection', e => log('unhandled: ' + (e.reason?.message ?? e.reason), 'bad'))

;(async () => {
  log('importing wasm…', 'info')
  const fkn = { net, dgram, storage: null }
  const inst = await (factory as any)({ fkn })
  ;(window as any).__inst = inst
  inst._lt_session_create()
  // Give session_impl::init a chance to run — it calls reopen_listen_sockets
  // which triggers the first socket() syscall, which is when FKN.init() runs
  // on the JS side. Without this, anything that touches inst.__FKN right after
  // session_create (like hookUdp below on first __add()) hits undefined.
  for (let i = 0; i < 30; i++) inst._lt_session_tick()
  log('session up', 'ok')

  // No setIntervals. Driving is manual via console:
  //   __tick(n)   — pump n ticks
  //   __drain()   — pull alerts, print them
  //   __add()     — add the magnet from the input
  //   __status()  — fds + tick stats
  //   __rx()      — show incoming UDP packets observed by the hook

  const rxLog: any[] = []
  ;(window as any).__rxLog = rxLog

  const hookUdp = () => {
    const fkn = (inst as any).__FKN
    if (!fkn) return false
    const udp = [...fkn.fds.values()].find((s: any) => s.kind === 'udp')
    if (!udp || !udp.socket) return false
    udp.socket.on('message', (data: any, rinfo: any) => {
      rxLog.push({ when: Date.now(), from: rinfo?.address + ':' + rinfo?.port, len: data?.length, head: Array.from((data || []).slice(0, 8)) })
    })
    return true
  }

  ;(window as any).__tick = (n = 50) => { for (let i = 0; i < n; i++) inst._lt_session_tick() }
  ;(window as any).__drain = () => {
    inst._lt_session_pump_alerts()
    const sz = inst._lt_alerts_size(); const ptr = inst._lt_alerts_data()
    const v = new DataView(inst.HEAPU8.buffer, ptr, sz)
    const a = []
    for (let o = 0; o < sz; ) {
      const t = v.getUint32(o, true); o += 4
      const l = v.getUint32(o, true); o += 4
      a.push({ t, m: inst.UTF8ToString(ptr + o, l) }); o += l
    }
    inst._lt_alerts_clear()
    return a
  }
  ;(window as any).__add = () => {
    const magnet = ($('magnet') as HTMLInputElement).value
    const mp = inst.stringToNewUTF8(magnet); const pp = inst.stringToNewUTF8('/dl')
    const rc = inst._lt_session_add_magnet(mp, pp)
    inst._free(mp); inst._free(pp)
    hookUdp()
    ;(inst as any).__FKN.scheduleTick()
    return rc
  }

  // Fallback heartbeat — libtorrent's internal timers (tracker retries,
  // unchoke, etc.) need someone to tick the io_context to fire. 1 Hz is
  // plenty for sub-minute cadences and won't pin the CPU.
  setInterval(() => (inst as any).__FKN.scheduleTick(), 1000)
  ;(window as any).__status = () => {
    const fkn = (inst as any).__FKN
    const fdsByKind: Record<string, number> = {}
    for (const st of fkn.fds.values()) fdsByKind[st.kind] = (fdsByKind[st.kind] || 0) + 1
    return {
      fds: fkn.fds.size,
      fdsByKind,
      ticks: Number(inst._lt_diag_tick_count()),
      handlers: Number(inst._lt_diag_total_handlers()),
      udp: { rx: fkn.stats.udpRx, tx: fkn.stats.udpTx },
      tcp: { rx: fkn.stats.tcpRx, tx: fkn.stats.tcpTx, recv: fkn.stats.recv, send: fkn.stats.send },
      pkts: { udpRx: rxLog.length },
    }
  }
  ;(window as any).__rx = () => rxLog

  $('add').addEventListener('click', () => log('add rc=' + (window as any).__add(), 'info'))
  $('stat').innerHTML = 'Driving manually — call <code>__add()</code>, <code>__tick()</code>, <code>__drain()</code>, <code>__status()</code>, <code>__rx()</code> from the console.'
  log('READY', 'ok')
})().catch(e => log('init FAIL: ' + (e?.stack ?? e), 'bad'))
