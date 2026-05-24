// Main thread: pipes UI events into the worker and renders updates.
// All the libtorrent work happens in the worker — keeping the worker
// single-threaded matches the wasm build (no SAB needed).

const worker = new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' })

const $ = (id: string) => document.getElementById(id)!

worker.onmessage = (ev) => {
  const msg = ev.data
  if (msg.type === 'status') {
    const s = msg.status
    const states = ['queued', 'checking', 'metadata', 'downloading', 'finished', 'seeding', 'allocating', 'check-resume']
    $('state').textContent = states[s.state] ?? `state-${s.state}`
    $('progress').textContent = (s.progress * 100).toFixed(1) + '%'
    $('down').textContent = (s.downloadPayloadRate / 1024).toFixed(1) + ' KB/s'
    $('up').textContent = (s.uploadPayloadRate / 1024).toFixed(1) + ' KB/s'
    $('peers').textContent = String(s.numPeers)
    $('seeds').textContent = String(s.numSeeds)
  } else if (msg.type === 'alert') {
    const el = $('alerts')
    el.textContent = (msg.line + '\n' + el.textContent).slice(0, 8000)
  }
}

$('add').addEventListener('click', () => {
  const magnet = ($('magnet') as HTMLInputElement).value.trim()
  if (!magnet) return
  worker.postMessage({ type: 'add-magnet', magnet })
})
