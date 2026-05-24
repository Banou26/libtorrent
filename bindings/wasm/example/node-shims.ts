// ES module imports are hoisted, so any shim has to live in a separately-
// imported file (which is itself hoisted, but runs ahead of imports in
// modules that import it). Both the main page and the worker pull this in
// before touching @fkn/lib.
//
// `process.nextTick` MUST forward trailing args — readable-stream calls
// `process.nextTick(resume_, stream, state)` and resume_ blows up with
// 'Cannot read properties of undefined (reading "reading")' otherwise.

const root: any = typeof globalThis !== 'undefined' ? globalThis : (typeof self !== 'undefined' ? self : (typeof window !== 'undefined' ? window : {}))

if (!root.global) root.global = root
if (!root.process) {
  root.process = {
    env: { NODE_DEBUG: '' },
    version: '',
    nextTick: (fn: any, ...args: any[]) => queueMicrotask(() => fn(...args)),
  }
}
