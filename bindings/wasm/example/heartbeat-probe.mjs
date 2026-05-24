// End-to-end smoke test: load live.html, add the magnet, watch for 30s.
// Polls window state every second from node-side so we get a heartbeat
// even if page-side timers are sad. Reports the final session stats.

import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'

const URL_ = 'http://localhost:4560/live.html'
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
  page.setDefaultTimeout(3000)
  page.on('console', m => {
    const t = m.text()
    // Only surface signal — drop the [vite] HMR chatter and the WASM startup spam.
    if (/^\[(vite|FKN)\]/.test(t) || /^\[lt\]/.test(t) || /unsupported syscall/.test(t)) return
    log('  console', m.type(), t)
  })
  let pageErrorSeen = 0
  page.on('pageerror', e => {
    pageErrorSeen++
    // First 3 errors get the full stack; rest get one-line dedup.
    if (pageErrorSeen <= 3) log('  PAGEERROR', e.message, '\n' + (e.stack || ''))
    else if (pageErrorSeen === 4) log('  PAGEERROR (more suppressed, same as above)')
  })

  await T('goto', page.goto(URL_, { waitUntil: 'commit' }), 8000)
  await T('wasm-up', page.waitForFunction(() => !!window.__inst, null, { timeout: 15000 }), 16000)
  log('WASM up — adding magnet…')
  await T('add', page.evaluate(() => window.__add()), 5000)

  for (let i = 1; i <= 30; i++) {
    await new Promise(r => setTimeout(r, 1000))
    try {
      const s = await T(`poll${i}`, page.evaluate(() => window.__status()), 1500)
      log(`t+${i}s`, JSON.stringify(s))
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
