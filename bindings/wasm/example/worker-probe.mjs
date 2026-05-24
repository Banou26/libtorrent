// Smoke test for index.html (worker + OPFS storage variant).

import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'

const URL_ = 'http://localhost:4560/'
const T = (label, p, ms) =>
  Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`TIMEOUT(${ms}ms) ${label}`)), ms))])
const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a)

const browser = await chromium.launch({
  executablePath: '/etc/profiles/per-user/banou/bin/google-chrome',
  headless: true,
  args: ['--ignore-certificate-errors', '--enable-experimental-web-platform-features', '--no-sandbox'],
})

let exit = 0
try {
  const ctx = await browser.newContext()
  const page = await ctx.newPage()
  page.setDefaultTimeout(5000)
  page.on('console', m => {
    const t = m.text()
    if (/^\[vite\]/.test(t)) return
    log('  console', m.type(), t)
  })
  let pageErrorSeen = 0
  page.on('pageerror', e => {
    pageErrorSeen++
    if (pageErrorSeen <= 3) log('  PAGEERROR', e.message, '\n' + (e.stack || ''))
    else if (pageErrorSeen === 4) log('  PAGEERROR (more suppressed, same as above)')
  })

  await T('goto', page.goto(URL_, { waitUntil: 'commit' }), 8000)
  await T('wait-ready', page.waitForFunction(() => document.getElementById('state')?.textContent === 'ready', null, { timeout: 15000 }), 16000)
  log('worker reports ready — adding magnet')

  // BBB + Ubuntu LTS — more peers usually means we don't have to wait
  // for one stalling tracker. Multiple trackers improves resilience.
  const magnet = process.argv[2] || 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=Big+Buck+Bunny&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A6969&tr=udp%3A%2F%2Fexodus.desync.com%3A6969'
  await page.evaluate((m) => {
    const inp = document.getElementById('magnet')
    inp.value = m
    document.getElementById('add').click()
  }, magnet)

  let prev = { ticks: 0, handlers: 0, recvfrom: 0, sendto: 0, recv: 0, send: 0, diskW: 0, poll: 0, udpRx: 0, tcpRx: 0 }
  for (let i = 1; i <= 30; i++) {
    await new Promise(r => setTimeout(r, 1000))
    try {
      const r = await T(`s${i}`, page.evaluate(() => window.__lastStatus || null), 2000)
      if (!r) { log(`t+${i}s`, 'no status yet'); continue }
      const dT = r.ticks - prev.ticks
      const dH = r.handlers - prev.handlers
      const dRf = r.recvfrom - prev.recvfrom
      const dSt = r.sendto - prev.sendto
      const dR = r.recv - prev.recv
      const dS = r.send - prev.send
      const dPoll = r.poll - prev.poll
      const dW = r.diskW - prev.diskW
      const dUdpRx = r.udp.rx - prev.udpRx
      const dTcpRx = r.tcp.rx - prev.tcpRx
      prev = { ticks: r.ticks, handlers: r.handlers, recvfrom: r.recvfrom, sendto: r.sendto, recv: r.recv, send: r.send, diskW: r.diskW, poll: r.poll, udpRx: r.udp.rx, tcpRx: r.tcp.rx }
      const d = r.tcpDetail || {}
      log(`t+${i}s`,
        `tk=${dT} hd=${dH} poll=${dPoll}`,
        `udp[rcv=${dRf}/snd=${dSt}] tcp[rcv=${dR}/snd=${dS}]`,
        `udpRx=${(dUdpRx/1024).toFixed(0)}KiB tcpRx=${(dTcpRx/1024).toFixed(0)}KiB dw=${dW}`,
        `tcp[c=${d.connected}/-ing=${d.connecting}/d=${d.withData}/e=${d.errored}]`,
        `tot=${((r.udp.rx+r.tcp.rx)/1024/1024).toFixed(2)}MiB`)
    } catch (e) {
      log(`t+${i}s FROZEN — ${e.message}`)
      break
    }
  }
} catch (e) {
  log('FATAL', e.message)
  exit = 1
} finally {
  await browser.close().catch(() => {})
  process.exit(exit)
}
