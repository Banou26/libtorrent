// libtorrent + @fkn/lib's @webvpn/{net,dgram} running in a Web Worker.
//
// Why: when libtorrent lived on the main thread, every WebVPN UDP datagram
// went through the @fkn/lib iframe → main-thread postMessage → my JS shim,
// all sharing the renderer's single JS thread. Bursts of incoming packets
// starved the libtorrent tick chain (and vice versa) and the renderer
// went unresponsive. With libtorrent in a Worker:
//   - The tick chain has its own JS thread (this Worker).
//   - The main thread keeps the FKN iframe and uses relayWorker() to bridge
//     osra messages between iframe and worker — so the worker's @fkn/lib
//     can call net/dgram transparently.

// Node-stdlib shims expected by @fkn/lib transitive deps (buffer, stream).
// In the Worker scope `self` is the global; mirror the live.html setup.
;(self as any).global = self
;(self as any).process = { env: { NODE_DEBUG: '' }, version: '', nextTick: (fn: any, ...args: any[]) => queueMicrotask(() => fn(...args)) }

import * as net from '@webvpn/net'
import * as dgram from '@webvpn/dgram'

import factory from './libtorrent.js'

let inst: any
const rxLog: any[] = []
let udpHooked = false

const hookUdp = () => {
  if (udpHooked) return
  const fkn = inst.__FKN
  if (!fkn) return
  const udp = [...fkn.fds.values()].find((s: any) => s.kind === 'udp')
  if (!udp || !udp.socket) return
  udp.socket.on('message', (data: any, rinfo: any) => {
    rxLog.push({ when: Date.now(), from: rinfo?.address + ':' + rinfo?.port, len: data?.length })
  })
  udpHooked = true
}

const drainAlerts = () => {
  inst._lt_session_pump_alerts()
  const sz = inst._lt_alerts_size()
  if (!sz) return []
  const ptr = inst._lt_alerts_data()
  const view = new DataView(inst.HEAPU8.buffer, ptr, sz)
  const out: { t: number; m: string }[] = []
  for (let o = 0; o < sz; ) {
    const t = view.getUint32(o, true); o += 4
    const l = view.getUint32(o, true); o += 4
    out.push({ t, m: inst.UTF8ToString(ptr + o, l) }); o += l
  }
  inst._lt_alerts_clear()
  return out
}

const status = () => ({
  fds: inst.__FKN ? inst.__FKN.fds.size : 0,
  ticks: Number(inst._lt_diag_tick_count()),
  handlers: Number(inst._lt_diag_total_handlers()),
  rxCount: rxLog.length,
})

const init = async () => {
  const fkn = { net, dgram, storage: null }
  inst = await (factory as any)({ fkn })
  inst._lt_session_create()
  // First handful of ticks: bring up listen sockets so FKN init runs.
  for (let i = 0; i < 30; i++) inst._lt_session_tick()
  // Fallback heartbeat — libtorrent's internal timers (tracker retries,
  // unchoke, etc.) need someone to tick the io_context to fire.
  setInterval(() => inst.__FKN?.scheduleTick(), 1000)
  ;(self as any).postMessage({ type: 'ready' })
}

self.onmessage = (e: MessageEvent) => {
  const m = e.data
  if (!inst) {
    ;(self as any).postMessage({ type: 'error', message: 'worker not initialized' })
    return
  }
  if (m.type === 'add-magnet') {
    const mp = inst.stringToNewUTF8(m.magnet)
    const pp = inst.stringToNewUTF8(m.savePath || '/dl')
    const rc = inst._lt_session_add_magnet(mp, pp)
    inst._free(mp); inst._free(pp)
    inst.__FKN.scheduleTick()
    hookUdp()
    ;(self as any).postMessage({ type: 'add-result', rc })
  } else if (m.type === 'poll') {
    ;(self as any).postMessage({ type: 'poll-result', status: status(), alerts: drainAlerts(), rx: rxLog.slice(-10) })
  }
}

init().catch((e: any) => {
  ;(self as any).postMessage({ type: 'error', message: String(e?.stack ?? e) })
})
