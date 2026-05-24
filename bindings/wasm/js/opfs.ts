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
    this.storages.set(id, { rootDir, files: new Map(), fileMeta: files })
  }

  async onRemoveStorage(id: number) {
    const e = this.storages.get(id)
    if (!e) return
    for (const h of e.files.values()) {
      try { h.close() } catch (e) {}
    }
    this.storages.delete(id)
  }

  async read(id: number, fileIndex: number, offset: number, len: number): Promise<Uint8Array> {
    const handle = await this.openFile(id, fileIndex)
    const out = new Uint8Array(len)
    const read = handle.read(out, { at: offset })
    if (read < len) {
      // Underlying file is shorter than requested (sparse area not yet
      // written). Zero-fill the rest — libtorrent treats short reads as
      // errors otherwise.
      out.fill(0, read)
    }
    return out
  }

  async write(id: number, fileIndex: number, offset: number, bytes: Uint8Array): Promise<void> {
    const handle = await this.openFile(id, fileIndex)
    handle.write(bytes, { at: offset })
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

  private async openFile(id: number, fileIndex: number): Promise<FileSystemSyncAccessHandle> {
    const e = this.storages.get(id)
    if (!e) throw new Error(`unknown storage ${id}`)
    let h = e.files.get(fileIndex)
    if (h) return h
    const meta = e.fileMeta[fileIndex]
    if (!meta) throw new Error(`unknown file ${fileIndex}`)
    const segments = meta.path.split('/').filter(Boolean)
    const name = segments.pop()!
    let dir = e.rootDir
    for (const s of segments) {
      dir = await dir.getDirectoryHandle(s, { create: true })
    }
    const fileHandle = await dir.getFileHandle(name, { create: true })
    h = await (fileHandle as any).createSyncAccessHandle()
    e.files.set(fileIndex, h!)
    return h!
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
