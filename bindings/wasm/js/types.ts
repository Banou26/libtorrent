// Type shape of the Emscripten-generated module. We only declare what
// `js/index.ts` actually uses; Emscripten exports many more symbols.

export interface LtModule {
  HEAPU8: Uint8Array
  HEAP32: Int32Array
  HEAPU32: Uint32Array

  _malloc(n: number): number
  _free(ptr: number): void
  stringToNewUTF8(s: string): number
  UTF8ToString(ptr: number, maxBytes?: number): string

  _lt_session_create(): number
  _lt_session_destroy(): void
  _lt_session_tick(): void
  _lt_session_pump_alerts(): void

  _lt_alerts_size(): number
  _lt_alerts_data(): number
  _lt_alerts_clear(): void

  _lt_session_add_magnet(magnet: number, savePath: number): number
  _lt_session_add_torrent_file(buf: number, len: number, savePath: number): number
  _lt_session_remove_torrent(handle: number): number

  _lt_torrent_status(handle: number, out: number): number
  _lt_torrent_infohash(handle: number, out: number): number
}

export type LtModuleFactory = (init?: Partial<{ fkn: FknHost; wasmBinary: ArrayBuffer }>) => Promise<LtModule>

export interface FknHost {
  net: any
  dgram: any
  storage: StorageBackend | null
  /**
   * Optional. When supplied, the WASM resolver routes DNS lookups through
   * this function (typically @fkn/lib's dnsLookup, which tunnels via
   * WebVPN). When omitted, the module falls back to plain fetch against
   * 1.1.1.1's DoH JSON endpoint.
   */
  dnsLookup?: (hostname: string, opts?: { family?: 0 | 4 | 6 })
    => Promise<{ address: string; family: 0 | 4 | 6 } | { address: string; family: 0 | 4 | 6 }[] | undefined>
}

// The disk-IO contract a host implements. Every method may return Promise.
// Methods unused by libtorrent in browser-mode are optional.
export interface StorageBackend {
  onNewStorage(id: number, savePath: string, files: Array<{ path: string; size: number }>): void | Promise<void>
  onRemoveStorage(id: number): void | Promise<void>

  read(id: number, fileIndex: number, offset: number, len: number): Promise<Uint8Array>
  write(id: number, fileIndex: number, offset: number, bytes: Uint8Array): Promise<void>

  release?(id: number): Promise<void>
  check?(id: number): Promise<number>  // status_t — 0 = no_error
  move?(id: number, newPath: string): Promise<void>
  deleteFiles?(id: number, flags: number): Promise<void>
  rename?(id: number, fileIndex: number, newName: string): Promise<void>
  stop?(id: number): Promise<void>
}
