import { chromium } from '/home/banou/dev/fkn/proxy/node_modules/playwright/index.mjs'

const browser = await chromium.launch({
  executablePath: '/etc/profiles/per-user/banou/bin/google-chrome',
  headless: true,
  args: [
    '--no-sandbox',
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--disable-backgrounding-occluded-windows',
  ],
})
try {
  const page = await browser.newPage()
  page.on('console', m => console.log(new Date().toISOString().slice(11, 23), m.type(), m.text()))
  page.on('pageerror', e => console.log(new Date().toISOString().slice(11, 23), 'PAGEERROR', e.message, e.stack))
  page.on('requestfailed', r => console.log(new Date().toISOString().slice(11, 23), 'REQFAIL', r.url(), r.failure()?.errorText))
  const target = process.argv[2] || 'timer-test.html'
  console.log('=== probing', target)
  await page.goto('http://localhost:4560/' + target, { waitUntil: 'commit' })
  await new Promise(r => setTimeout(r, 5000))
} finally {
  await browser.close()
}
