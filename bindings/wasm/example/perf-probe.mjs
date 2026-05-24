// Drive live.html via raw playwright. Every page call goes through a
// hard wall-clock timeout so a frozen renderer can't hang this script.
//
// Usage: node perf-probe.mjs

import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'
import { writeFile } from 'node:fs/promises'

const URL_ = 'http://localhost:4560/live.html'
const SHOT_DIR = '/tmp/lt-shots'

const T = (label, p, ms) =>
  Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`TIMEOUT(${ms}ms) ${label}`)), ms))
  ])

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

  page.on('console', m => log('  console', m.type(), m.text()))
  page.on('pageerror', e => log('  pageerror', e.message))

  log('NAV')
  await T('goto', page.goto(URL_, { waitUntil: 'commit' }), 8000)
  await T('wasm-up', page.waitForFunction(() => !!window.__inst, null, { timeout: 15000 }), 16000)
  log('WASM up')

  const probe = async (label) => {
    const tries = []
    // Probe A: trivial — does eval itself work?
    try {
      const a = await T(`${label}/trivial`, page.evaluate(() => 1+1), 1500)
      tries.push(`trivial=${a}`)
    } catch (e) { tries.push(`trivial=${e.message}`) }
    // Probe B: read-only window field — no FKN touched
    try {
      const b = await T(`${label}/url`, page.evaluate(() => document.location.href.length), 1500)
      tries.push(`url=${b}`)
    } catch (e) { tries.push(`url=${e.message}`) }
    // Probe C: status (touches __FKN.fds.size + diag counters — no syscalls)
    try {
      const c = await T(`${label}/status`, page.evaluate(() => window.__status?.()), 1500)
      tries.push(`status=${JSON.stringify(c)}`)
    } catch (e) { tries.push(`status=${e.message}`) }
    // Probe D: rx length
    try {
      const d = await T(`${label}/rx`, page.evaluate(() => window.__rx?.()?.length), 1500)
      tries.push(`rx=${d}`)
    } catch (e) { tries.push(`rx=${e.message}`) }
    return tries
  }

  log('PRE-ADD', (await probe('pre')).join(' | '))

  log('ADD')
  const addRc = await T('add', page.evaluate(() => window.__add()), 5000).catch(e => 'FAIL:'+e.message)
  log('  add rc:', addRc)

  for (let i = 0; i < 8; i++) {
    await new Promise(r => setTimeout(r, 700))
    const results = await probe(`t${i+1}`)
    log(`t+${(i+1)*0.7}s`, results.join(' | '))
  }
} catch (e) {
  log('FATAL', e.message)
  exit = 1
} finally {
  await browser.close().catch(() => {})
  process.exit(exit)
}
