// OPFS-backed StorageBackend. Each torrent gets a directory under the OPFS
// root; each file becomes one OPFS file. Reads use FileSystemFileHandle's
// .read at offset; writes use FileSystemSyncAccessHandle which is sync &
// fast — but only available inside Workers.
//
// Run from a Worker. Calling this from the main thread will throw.

import type { StorageBackend } from './types'

interface StorageEntry {
  rootDir: FileSystemDirectoryHandle
  files: Map<number, FileSystemSyncAccessHandle>
  // In-flight opens: we cache the Promise so two callers racing for the
  // same file resolve to the same handle. OPFS only allows one
  // SyncAccessHandle per file, so two concurrent createSyncAccessHandle()
  // calls would throw "A FileSystemSyncAccessHandle for the same file
  // has been created in this scope" on the second one.
  opening: Map<number, Promise<FileSystemSyncAccessHandle>>
  fileMeta: Array<{ path: string; size: number }>
}

export class OPFSStorage implements StorageBackend {
  private storages = new Map<number, StorageEntry>()

  async onNewStorage(id: number, savePath: string, files: Array<{ path: string; size: number }>) {
    const root = await navigator.storage.getDirectory()
    // Mirror savePath into OPFS: stripping any leading slash, otherwise
    // getDirectoryHandle complains.
    const cleanPath = savePath.replace(/^\/+/, '')
    const rootDir = await ensureDirRecursive(root, cleanPath)
    this.storages.set(id, { rootDir, files: new Map(), opening: new Map(), fileMeta: files })
  }

  async onRemoveStorage(id: number) {
    const e = this.storages.get(id)
    if (!e) return
    for (const h of e.files.values()) {
      try { h.close() } catch (e) {}
    }
    this.storages.delete(id)
  }

  // Note: return type is a union of sync and Promise — js_disk_read/write
  // detect this and skip the microtask round-trip when the file handle is
  // already cached, which is the steady-state hot path during streaming.
  read(id: number, fileIndex: number, offset: number, len: number): Uint8Array | Promise<Uint8Array> {
    const handleOrPromise = this.openFile(id, fileIndex)
    const doRead = (handle: FileSystemSyncAccessHandle): Uint8Array => {
      const out = new Uint8Array(len)
      const read = handle.read(out, { at: offset })
      if (read < len) out.fill(0, read)
      return out
    }
    if (handleOrPromise instanceof Promise) return handleOrPromise.then(doRead)
    return doRead(handleOrPromise)
  }

  write(id: number, fileIndex: number, offset: number, bytes: Uint8Array): void | Promise<void> {
    const handleOrPromise = this.openFile(id, fileIndex)
    const doWrite = (handle: FileSystemSyncAccessHandle): void => {
      try {
        const wrote = handle.write(bytes, { at: offset })
        if (wrote < bytes.length) {
          // eslint-disable-next-line no-console
          console.error('[opfs] short write', { id, fileIndex, offset, want: bytes.length, wrote })
          throw new Error('short write')
        }
      } catch (e) {
        // eslint-disable-next-line no-console
        console.error('[opfs] write failed', {
          id, fileIndex, offset, len: bytes.length,
          err: String(e),
          errName: (e as any)?.name,
          errMsg: (e as any)?.message,
          isView: ArrayBuffer.isView(bytes),
          bytesType: Object.prototype.toString.call(bytes),
        })
        throw e
      }
    }
    if (handleOrPromise instanceof Promise) {
      return handleOrPromise.then(doWrite).catch((e) => {
        // eslint-disable-next-line no-console
        console.error('[opfs] openFile failed', { id, fileIndex, err: String(e) })
        throw e
      })
    }
    return doWrite(handleOrPromise)
  }

  async release(id: number): Promise<void> {
    const e = this.storages.get(id)
    if (!e) return
    for (const h of e.files.values()) {
      try { h.flush(); h.close() } catch (e) {}
    }
    e.files.clear()
  }

  async stop(id: number): Promise<void> {
    return this.release(id)
  }

  async check(_id: number): Promise<number> {
    // 0 = status_t::no_error. The session will re-hash to verify.
    return 0
  }

  async deleteFiles(id: number, _flags: number): Promise<void> {
    const e = this.storages.get(id)
    if (!e) return
    for (const h of e.files.values()) try { h.close() } catch {}
    e.files.clear()
    for (const meta of e.fileMeta) {
      const segments = meta.path.split('/').filter(Boolean)
      const name = segments.pop()
      if (!name) continue
      let dir = e.rootDir
      for (const s of segments) {
        try { dir = await dir.getDirectoryHandle(s) } catch { dir = null as any; break }
      }
      if (dir) try { await dir.removeEntry(name) } catch {}
    }
  }

  // Returns the cached SyncAccessHandle synchronously when it exists,
  // otherwise a Promise that resolves to one. Hot-path reads/writes pay
  // zero microtask cost once the handle has been opened the first time.
  // Concurrent callers for the same file share one in-flight Promise so
  // we never invoke createSyncAccessHandle() twice for the same file —
  // the second call would throw the "same file" lock error.
  private openFile(id: number, fileIndex: number): FileSystemSyncAccessHandle | Promise<FileSystemSyncAccessHandle> {
    const e = this.storages.get(id)
    if (!e) throw new Error(`unknown storage ${id}`)
    const cached = e.files.get(fileIndex)
    if (cached) return cached
    const pending = e.opening.get(fileIndex)
    if (pending) return pending
    const p = this.openFileSlow(id, fileIndex)
    e.opening.set(fileIndex, p)
    p.finally(() => e.opening.delete(fileIndex))
    return p
  }

  private async openFileSlow(id: number, fileIndex: number): Promise<FileSystemSyncAccessHandle> {
    const e = this.storages.get(id)!
    const meta = e.fileMeta[fileIndex]
    if (!meta) throw new Error(`unknown file ${fileIndex}`)
    const segments = meta.path.split('/').filter(Boolean)
    const name = segments.pop()!
    let dir = e.rootDir
    for (const s of segments) {
      dir = await dir.getDirectoryHandle(s, { create: true })
    }
    const fileHandle = await dir.getFileHandle(name, { create: true })
    const h = await (fileHandle as any).createSyncAccessHandle() as FileSystemSyncAccessHandle
    e.files.set(fileIndex, h)
    return h
  }
}

async function ensureDirRecursive(
  root: FileSystemDirectoryHandle, path: string,
): Promise<FileSystemDirectoryHandle> {
  if (!path) return root
  let dir = root
  for (const seg of path.split('/').filter(Boolean)) {
    dir = await dir.getDirectoryHandle(seg, { create: true })
  }
  return dir
}

// minimal typings — TS lib.dom doesn't yet ship Sync handle methods fully
declare global {
  interface FileSystemSyncAccessHandle {
    read(buf: BufferSource, opts?: { at?: number }): number
    write(buf: BufferSource, opts?: { at?: number }): number
    flush(): void
    close(): void
    truncate(size: number): void
    getSize(): number
  }
}
