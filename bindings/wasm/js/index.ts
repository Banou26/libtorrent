// Public API for the libtorrent WASM module.
//
// Usage from a Web Worker (recommended — keeps the main thread responsive):
//
//   import { createSession } from 'libtorrent-wasm'
//   import * as net from '@webvpn/net'
//   import * as dgram from '@webvpn/dgram'
//   import { OPFSStorage } from 'libtorrent-wasm/opfs'
//
//   const session = await createSession({
//     net,
//     dgram,
//     storage: new OPFSStorage(),
//   })
//
//   const handle = session.addMagnet('magnet:?xt=urn:btih:…', '/downloads')
//   for await (const alert of session.alerts()) {
//     if (alert.type === ALERT_TYPES.state_changed) { ... }
//   }
//
// The session owns its own tick scheduler — it pumps on a microtask whenever
// the C side signals via Module.fkn.scheduleTick(), and on a 250 ms timer as
// a fallback for internal libtorrent timers (DHT bucket refresh, tracker
// announces, etc).

import type { LtModuleFactory, LtModule, FknHost, StorageBackend } from './types'

export interface SessionOptions {
  /** @fkn/lib's net module (or @webvpn/net) */
  net: any
  /** @fkn/lib's dgram module (or @webvpn/dgram) */
  dgram: any
  /** Optional disk backend — defaults to a no-op (download but discard) */
  storage?: StorageBackend
  /** Override the WASM module factory (testing) */
  moduleFactory?: LtModuleFactory
  /** Fallback tick interval in ms — used when nothing else pumps */
  tickIntervalMs?: number
}

export interface TorrentStatus {
  state: number
  paused: boolean
  progress: number
  totalDownload: bigint
  totalUpload: bigint
  totalDone: bigint
  totalWanted: bigint
  downloadPayloadRate: number
  uploadPayloadRate: number
  numPeers: number
  numSeeds: number
  numPieces: number
  hasMetadata: boolean
}

export interface Alert {
  type: number
  message: string
}

export class Session {
  private mod: LtModule
  private destroyed = false
  private fallbackTimer?: number

  private alertSubscribers: Array<(a: Alert) => void> = []

  constructor(mod: LtModule, options: SessionOptions) {
    this.mod = mod
    if (mod._lt_session_create() !== 0) {
      throw new Error('lt_session_create returned non-zero')
    }
    const tickMs = options.tickIntervalMs ?? 250
    // Pump even when no I/O happened — libtorrent has internal timers we
    // wouldn't otherwise honour. 250 ms is generous; tracker/DHT cadences
    // are seconds.
    this.fallbackTimer = setInterval(() => this.tick(), tickMs) as unknown as number
  }

  addMagnet(magnet: string, savePath: string = '/downloads'): number {
    const m = this.mod
    const magnetPtr = m.stringToNewUTF8(magnet)
    const pathPtr = m.stringToNewUTF8(savePath)
    try {
      return m._lt_session_add_magnet(magnetPtr, pathPtr) >>> 0
    } finally {
      m._free(magnetPtr); m._free(pathPtr)
    }
  }

  addTorrentFile(buffer: Uint8Array, savePath: string = '/downloads'): number {
    const m = this.mod
    const ptr = m._malloc(buffer.length)
    m.HEAPU8.set(buffer, ptr)
    const pathPtr = m.stringToNewUTF8(savePath)
    try {
      return m._lt_session_add_torrent_file(ptr, buffer.length, pathPtr) >>> 0
    } finally {
      m._free(ptr); m._free(pathPtr)
    }
  }

  removeTorrent(handle: number) {
    this.mod._lt_session_remove_torrent(handle)
  }

  // Layout matches torrent_status_out in wrapper.cpp. Cheap enough to call
  // every tick from a UI.
  status(handle: number): TorrentStatus | null {
    const m = this.mod
    const ptr = m._malloc(96)
    try {
      if (m._lt_torrent_status(handle, ptr) !== 0) return null
      const dv = new DataView(m.HEAPU8.buffer, ptr, 96)
      let o = 0
      const i32 = () => { const v = dv.getInt32(o, true); o += 4; return v }
      const f32 = () => { const v = dv.getFloat32(o, true); o += 4; return v }
      const i64 = () => { const v = dv.getBigInt64(o, true); o += 8; return v }
      const state = i32()
      const paused = i32() !== 0
      const progress = f32()
      const totalDownload = i64()
      const totalUpload = i64()
      const totalDone = i64()
      const totalWanted = i64()
      i64() // total_payload_download
      i64() // total_payload_upload
      const _dr = i32(); void _dr
      const _ur = i32(); void _ur
      const downloadPayloadRate = i32()
      const uploadPayloadRate = i32()
      const numPeers = i32()
      const numSeeds = i32()
      const numPieces = i32()
      i32() // num_connections
      const hasMetadata = i32() !== 0
      return {
        state, paused, progress,
        totalDownload, totalUpload, totalDone, totalWanted,
        downloadPayloadRate, uploadPayloadRate,
        numPeers, numSeeds, numPieces, hasMetadata,
      }
    } finally {
      m._free(ptr)
    }
  }

  infohash(handle: number): string | null {
    const m = this.mod
    const ptr = m._malloc(41)
    try {
      if (m._lt_torrent_infohash(handle, ptr) !== 0) return null
      return m.UTF8ToString(ptr)
    } finally {
      m._free(ptr)
    }
  }

  // Pull all currently-pending alerts and yield them. Returns immediately
  // when none are queued (so it's safe to call in a busy loop).
  *popAlerts(): IterableIterator<Alert> {
    const m = this.mod
    m._lt_session_pump_alerts()
    const size = m._lt_alerts_size() >>> 0
    if (!size) return
    const start = m._lt_alerts_data() >>> 0
    const view = new DataView(m.HEAPU8.buffer, start, size)
    let off = 0
    while (off < size) {
      const type = view.getUint32(off, true); off += 4
      const len = view.getUint32(off, true); off += 4
      const msg = m.UTF8ToString(start + off, len); off += len
      yield { type, message: msg }
    }
    m._lt_alerts_clear()
  }

  // Async iterator over alerts — pumps until destroy().
  async *alerts(): AsyncIterableIterator<Alert> {
    while (!this.destroyed) {
      for (const a of this.popAlerts()) yield a
      // Yield to the event loop so JS-side handlers and the fallback
      // timer have a chance to run.
      await new Promise<void>(r => setTimeout(r, 100))
    }
  }

  tick() {
    if (this.destroyed) return
    this.mod._lt_session_tick()
  }

  destroy() {
    if (this.destroyed) return
    this.destroyed = true
    if (this.fallbackTimer != null) clearInterval(this.fallbackTimer)
    this.mod._lt_session_destroy()
  }
}

export async function createSession(options: SessionOptions): Promise<Session> {
  const factory = options.moduleFactory
    ?? (await import('../build/libtorrent.js')).default as LtModuleFactory

  // Build the FKN host object the JS library reads on init.
  const host: FknHost = {
    net: options.net,
    dgram: options.dgram,
    storage: options.storage ?? null,
  }
  const mod: LtModule = await factory({ fkn: host })
  return new Session(mod, options)
}
