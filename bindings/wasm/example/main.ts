// Main thread: hosts the @fkn/lib iframe (auto-injected on import), bridges
// osra messages to the worker via relayWorker, pipes UI events through to
// the worker, and renders status updates back. All libtorrent + WASM work
// happens in the worker — the worker is single-threaded which matches the
// wasm build (no SAB needed).

import { relayWorker } from '@fkn/lib'

const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' })

// Wait one tick for the auto-injected iframe's contentWindow to attach,
// then bridge osra messages between the iframe and the worker. Without
// this the worker's @webvpn/{net,dgram} calls have no transport.
setTimeout(() => {
  try { relayWorker(worker) }
  catch (e) { console.error('relayWorker failed:', e) }
}, 100)

const $ = (id: string) => document.getElementById(id)!

let lastUdpRx = 0, lastUdpTx = 0, lastTs = Date.now()
const fmtRate = (bytes: number) => {
  if (bytes < 1024) return bytes + ' B/s'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KiB/s'
  return (bytes / (1024 * 1024)).toFixed(2) + ' MiB/s'
}

// addEventListener — safer than `worker.onmessage =` when @fkn/lib's
// relayWorker is also wiring listeners on this worker.
worker.addEventListener('message', (ev: MessageEvent) => {
  const msg = ev.data
  if (msg.type === 'ready') {
    $('state').textContent = 'ready'
  } else if (msg.type === 'worker-error') {
    console.error('[worker]', ...(msg.args || []))
    ;(window as any).__workerErrors = (window as any).__workerErrors || []
    ;(window as any).__workerErrors.push(msg.args)
  } else if (msg.type === 'poll-result') {
    const s = msg.status
    if (!s.ready) return
    ;(window as any).__lastStatus = s
    const now = Date.now()
    const dt = (now - lastTs) / 1000
    const rxRate = dt > 0 ? (s.udp.rx - lastUdpRx) / dt : 0
    const txRate = dt > 0 ? (s.udp.tx - lastUdpTx) / dt : 0
    lastUdpRx = s.udp.rx; lastUdpTx = s.udp.tx; lastTs = now
    $('state').textContent = `${s.fds} fds  ${s.fdsByKind?.tcp ?? 0} tcp / ${s.fdsByKind?.udp ?? 0} udp / ${s.fdsByKind?.['tcp-listen'] ?? 0} listen`
    $('progress').textContent = `${s.ticks} ticks, ${s.handlers} handlers`
    $('down').textContent = fmtRate(rxRate)
    $('up').textContent = fmtRate(txRate)
    $('peers').textContent = String(s.udpPkts)
    $('seeds').textContent = `${(s.udp.rx / 1024).toFixed(0)} KiB`
    for (const a of msg.alerts || []) {
      // 79/80 are alert::session_log and alert::torrent_log — they fire
      // every tick once a torrent is active and bury the interesting
      // stuff. 57 is stats. Filter from DOM but pipe to console so
      // probes can grep the wire.
      if (a.t === 79 || a.t === 80 || a.t === 57) continue
      console.log('ALERT', a.t, a.m)
      const el = $('alerts')
      el.textContent = (`[${a.t}] ${a.m}\n` + el.textContent).slice(0, 8000)
    }
  }
})

$('add').addEventListener('click', () => {
  const magnet = ($('magnet') as HTMLInputElement).value.trim()
  if (!magnet) return
  worker.postMessage({ type: 'add-magnet', magnet, savePath: '/dl' })
})

// Periodically ask the worker for status — sub-second cadence isn't useful
// here and would just generate postMessage chatter.
setInterval(() => worker.postMessage({ type: 'poll' }), 1000)
