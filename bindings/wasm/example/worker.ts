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

// Node-stdlib shims (global, process). Imported separately so it runs
// BEFORE the hoisted @webvpn/{net,dgram} imports — those transitively
// pull readable-stream which dereferences `process` at module-eval time.
import './node-shims'

import * as net from '@webvpn/net'
import * as dgram from '@webvpn/dgram'

import factory from './libtorrent.js'
import { OPFSStorage } from '../js/opfs'

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

const status = () => {
  const fkn = inst.__FKN
  if (!fkn) return { ready: false }
  const fdsByKind: Record<string, number> = {}
  let tcpConnected = 0, tcpConnecting = 0, tcpWithData = 0, tcpInError = 0
  // Aggregate per-fd diag across all TCP fds so we can see whether
  // anything is being polled / read / written at all.
  let tcpEverPolled = 0, tcpEverDataChunk = 0, tcpEverSendCall = 0, tcpEverRecvCall = 0
  let connDelayMaxMs = 0, connDelayCount = 0
  const tcpSample: any[] = []
  for (const st of fkn.fds.values()) {
    fdsByKind[st.kind] = (fdsByKind[st.kind] || 0) + 1
    if (st.kind === 'tcp') {
      if (st.connected) tcpConnected++
      else if (st.connecting) tcpConnecting++
      if (st.recv?.total > 0) tcpWithData++
      if (st.error) tcpInError++
      const d = st.diag
      if (d) {
        if (d.polled > 0) tcpEverPolled++
        if (d.dataChunks > 0) tcpEverDataChunk++
        if (d.sendCalls > 0) tcpEverSendCall++
        if (d.recvCalls > 0) tcpEverRecvCall++
        if (d.connectedAt && d.connectAt) {
          const delay = d.connectedAt - d.connectAt
          if (delay > connDelayMaxMs) connDelayMaxMs = delay
          connDelayCount++
        }
        if (tcpSample.length < 5 && st.connected) {
          tcpSample.push({
            connDelayMs: d.connectedAt - d.connectAt,
            polled: d.polled, polledOut: d.polledOut, polledIn: d.polledIn,
            sendCalls: d.sendCalls, recvCalls: d.recvCalls,
            dataChunks: d.dataChunks,
            tcpTxBytes: d.tcpTxBytes,
            firstSendLen: d.firstSendLen,
            recvTotal: st.recv?.total,
            nonblock: d.nonblockAtConnect,
            error: st.error,
          })
        }
      }
    }
  }
  return {
    tcpDetail: { connected: tcpConnected, connecting: tcpConnecting, withData: tcpWithData, errored: tcpInError },
    tcpEver: { polled: tcpEverPolled, dataChunk: tcpEverDataChunk, sendCall: tcpEverSendCall, recvCall: tcpEverRecvCall },
    tcpConnDelay: { count: connDelayCount, maxMs: connDelayMaxMs },
    tcpSample,
    tcpPolledConnected: fkn.stats._tcpPolledConnected || 0,
    tcpPolledConnecting: fkn.stats._tcpPolledConnecting || 0,
    tcpPolledOut: fkn.stats._tcpPolledOut || 0,
    tcpPolledIn: fkn.stats._tcpPolledIn || 0,
    sendBadFd: fkn.stats._sendBadFd || 0,
    sendNotConn: fkn.stats._sendNotConn || 0,
    writeBackpressure: fkn.stats._writeBackpressure || 0,
    tcpSendmsgCalls: fkn.stats._tcpSendmsgCalls || 0,
    ready: true,
    fds: fkn.fds.size,
    fdsByKind,
    ticks: Number(inst._lt_diag_tick_count()),
    handlers: Number(inst._lt_diag_total_handlers()),
    poll: fkn.stats.poll,
    pollReady: fkn.stats.pollReady,
    recvfrom: fkn.stats.recvfrom,
    sendto: fkn.stats.sendto,
    recv: fkn.stats.recv,
    send: fkn.stats.send,
    diskW: fkn.stats.diskWrite,
    diskR: fkn.stats.diskRead,
    udp: { rx: fkn.stats.udpRx, tx: fkn.stats.udpTx },
    tcp: { rx: fkn.stats.tcpRx, tx: fkn.stats.tcpTx },
    udpPkts: rxLog.length,
  }
}

const init = async () => {
  // OPFS is available in workers; using it as our disk backend means
  // libtorrent actually persists pieces (vs the live.html null-storage path
  // that discards everything just to keep the protocol happy).
  // Forward worker-level errors to the main thread for easier debugging.
  const origErr = console.error.bind(console)
  console.error = (...args: any[]) => {
    origErr(...args)
    try {
      ;(self as any).postMessage({ type: 'worker-error', args: args.map(a => {
        if (typeof a === 'object') {
          try { return JSON.stringify(a) } catch { return String(a) }
        }
        return String(a)
      }) })
    } catch {}
  }
  const storage = new OPFSStorage()
  const fkn = { net, dgram, storage }
  inst = await (factory as any)({ fkn })
  inst._lt_session_create()
  // First handful of ticks: bring up listen sockets so FKN init runs.
  for (let i = 0; i < 30; i++) inst._lt_session_tick()
  // Fallback heartbeat — libtorrent's internal timers (tracker retries,
  // unchoke, etc.) need someone to tick the io_context to fire.
  setInterval(() => inst.__FKN?.scheduleTick(), 1000)
  ;(self as any).postMessage({ type: 'ready' })
}

// addEventListener (not self.onmessage = …) so we coexist with @fkn/lib's
// relayWorker listener — assigning the property would clobber whichever
// listener was set last.
self.addEventListener('message', (e: MessageEvent) => {
  const m = e.data
  // Skip osra-shaped messages (those go to @fkn/lib's listener — they
  // have a specific envelope shape and we'd misinterpret them).
  if (!m || typeof m !== 'object' || !m.type || typeof m.type !== 'string') return
  if (m.type !== 'add-magnet' && m.type !== 'poll') return
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
})

init().catch((e: any) => {
  ;(self as any).postMessage({ type: 'error', message: String(e?.stack ?? e) })
})
