// Dump every alert libtorrent emits during a 30s run.

import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'

const browser = await chromium.launch({
  executablePath: '/etc/profiles/per-user/banou/bin/google-chrome',
  headless: true,
  args: ['--ignore-certificate-errors', '--enable-experimental-web-platform-features', '--no-sandbox'],
})
const ctx = await browser.newContext()
const page = await ctx.newPage()
page.setDefaultTimeout(5000)

page.on('console', m => {
  const t = m.text()
  if (/^\[(vite|FKN)\]/.test(t)) return
  if (/unsupported syscall/.test(t)) return
  console.log(t.slice(0, 500))
})
page.on('pageerror', e => console.log('PAGEERROR', e.message))

await page.goto('http://localhost:4560/', { waitUntil: 'commit' })
await page.waitForFunction(() => document.getElementById('state')?.textContent === 'ready', null, { timeout: 15000 })

await page.evaluate(() => {
  document.getElementById('magnet').value = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A6969&tr=udp%3A%2F%2Fexodus.desync.com%3A6969'
  document.getElementById('add').click()
})

await new Promise(r => setTimeout(r, 30_000))
await browser.close()
