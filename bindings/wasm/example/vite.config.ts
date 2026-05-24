import { defineConfig } from 'vite'
// @ts-ignore — JS module without types
import nodeStdlibBrowser from './vite-plugin-node-stdlib-browser.js'

export default defineConfig({
  server: {
    port: 4560,
    // No COOP/COEP — we don't use SharedArrayBuffer, and setting them
    // blocks the cross-origin iframe to fkn/web at :1234 (the iframe
    // needs CORP headers we don't control).
  },
  // The WASM file is built outside of Vite (by emcmake). Tell Vite to copy
  // it through as a static asset.
  assetsInclude: ['**/*.wasm'],
  optimizeDeps: {
    // Force pre-bundling for the @webvpn polyfills and @fkn/lib so the
    // node-stdlib shims wrap them up front.
    include: ['@webvpn/net', '@webvpn/dgram', '@fkn/lib'],
    // Don't try to optimize the wasm-side libtorrent.js (it isn't an npm
    // dep — it's the Emscripten output we pull in by relative path).
    exclude: ['libtorrent.js'],
  },
  plugins: [
    nodeStdlibBrowser(),
  ],
})
