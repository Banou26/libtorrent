// Boot the page, download for N seconds, then ask the worker to dump
// what's in OPFS so we can confirm bytes are actually persisted.

import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'

const browser = await chromium.launch({
  executablePath: '/etc/profiles/per-user/banou/bin/google-chrome',
  headless: true,
  args: ['--ignore-certificate-errors', '--enable-experimental-web-platform-features', '--no-sandbox'],
})

const ctx = await browser.newContext()
const page = await ctx.newPage()
page.on('console', m => { if (!/^\[vite\]/.test(m.text())) console.log(m.type(), m.text()) })

await page.goto('http://localhost:4560/', { waitUntil: 'commit' })
await page.waitForFunction(() => document.getElementById('state')?.textContent === 'ready', null, { timeout: 15000 })

await page.evaluate(() => {
  document.getElementById('magnet').value = 'magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=Big+Buck+Bunny&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337'
  document.getElementById('add').click()
})

console.log('downloading for 30s…')
await new Promise(r => setTimeout(r, 30_000))

// Walk OPFS root from main thread (workers and main share the same OPFS).
const tree = await page.evaluate(async () => {
  const root = await navigator.storage.getDirectory()
  const out = []
  const walk = async (dir, prefix) => {
    for await (const [name, handle] of dir) {
      const path = prefix + name
      if (handle.kind === 'file') {
        const f = await handle.getFile()
        out.push({ path, size: f.size })
      } else {
        await walk(handle, path + '/')
      }
    }
  }
  try { await walk(root, '') } catch (e) { return { error: e.message } }
  return out
})
console.log('\nOPFS contents:')
console.log(JSON.stringify(tree, null, 2))

await browser.close()
