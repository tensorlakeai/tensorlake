//
// TLFS — the production TensorLake FSKit module.
//
// A thin, sandboxed Swift proxy: every FSVolume operation translates onto the tl mount daemon's
// VFS wire protocol over localhost TCP (see crates/cli/src/commands/fs/vfsserver.rs, the
// authoritative protocol definition). The daemon owns the real filesystem — the gsvc-mount core
// (lazy server reads, immutable caches, workspace-ref following) and the writable overlay — so
// this extension holds no state beyond the item table and a small connection pool.
//
// Mount URLs: tlfs://127.0.0.1:<port>/<secret>
//
// Packaging (validated in ../fskit-hello): swiftc-built appex, -e _NSExtensionMain entry,
// EXExtensionPrincipalClass = TLFSFileSystem, embedded provisioning profile carrying
// com.apple.developer.fskit.fsmodule.
//
import Darwin
import ExtensionFoundation
import FSKit
import Foundation
import OSLog

let log = Logger(subsystem: "ai.tensorlake.tlfs", category: "fsmodule")

// MARK: - Wire protocol client

enum VfsOp: UInt8 {
    case hello = 0
    case getattr = 1
    case lookup = 2
    case forget = 3
    case opendir = 4
    case readdir = 5
    case releasedir = 6
    case open = 7
    case read = 8
    case write = 9
    case release = 10
    case fsync = 11
    case create = 12
    case mkdir = 13
    case symlink = 14
    case readlink = 15
    case unlink = 16
    case rmdir = 17
    case rename = 18
    case setattr = 19
    case statfs = 20
}

let vfsProtocolVersion: UInt32 = 1

struct WireWriter {
    var data = Data()
    mutating func u8(_ v: UInt8) { data.append(v) }
    mutating func u32(_ v: UInt32) { withUnsafeBytes(of: v.littleEndian) { data.append(contentsOf: $0) } }
    mutating func u64(_ v: UInt64) { withUnsafeBytes(of: v.littleEndian) { data.append(contentsOf: $0) } }
    mutating func bytes(_ v: Data) {
        u32(UInt32(v.count))
        data.append(v)
    }
    mutating func str(_ v: String) { bytes(Data(v.utf8)) }
}

struct WireReader {
    let data: Data
    var pos: Int
    init(_ data: Data) {
        self.data = data
        self.pos = data.startIndex
    }
    mutating func u8() throws -> UInt8 {
        guard pos < data.endIndex else { throw POSIXError(.EIO) }
        defer { pos += 1 }
        return data[pos]
    }
    mutating func u16() throws -> UInt16 {
        try fixed(2).withUnsafeBytes { $0.loadUnaligned(as: UInt16.self).littleEndian }
    }
    mutating func u32() throws -> UInt32 {
        try fixed(4).withUnsafeBytes { $0.loadUnaligned(as: UInt32.self).littleEndian }
    }
    mutating func i32() throws -> Int32 {
        try Int32(bitPattern: u32())
    }
    mutating func u64() throws -> UInt64 {
        try fixed(8).withUnsafeBytes { $0.loadUnaligned(as: UInt64.self).littleEndian }
    }
    mutating func bytes() throws -> Data {
        let len = Int(try u32())
        return try fixed(len)
    }
    mutating func str() throws -> String {
        guard let s = String(data: try bytes(), encoding: .utf8) else { throw POSIXError(.EIO) }
        return s
    }
    private mutating func fixed(_ n: Int) throws -> Data {
        guard pos + n <= data.endIndex else { throw POSIXError(.EIO) }
        defer { pos += n }
        return data.subdata(in: pos..<(pos + n))
    }
}

/// Attributes as encoded on the wire.
struct WireAttr {
    let ino: UInt64
    let kind: UInt8 // 0 dir, 1 file, 2 symlink
    let size: UInt64
    let perm: UInt16
    let upper: Bool

    init(_ r: inout WireReader) throws {
        ino = try r.u64()
        kind = try r.u8()
        size = try r.u64()
        perm = try r.u16()
        upper = try r.u8() != 0
    }

    var itemType: FSItem.ItemType {
        switch kind {
        case 0: return .directory
        case 2: return .symlink
        default: return .file
        }
    }
}

/// One blocking TCP connection: serial request/response with length framing.
final class VfsConnection {
    private let fd: Int32

    init(host: String, port: UInt16, secret: String) throws {
        fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw POSIXError(.EIO) }
        var addr = sockaddr_in()
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = port.bigEndian
        addr.sin_addr.s_addr = inet_addr(host)
        let rc = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                Darwin.connect(fd, sa, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard rc == 0 else {
            Darwin.close(fd)
            throw POSIXError(.ECONNREFUSED)
        }
        var one: Int32 = 1
        _ = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, socklen_t(MemoryLayout<Int32>.size))

        var w = WireWriter()
        w.str(secret)
        w.u32(vfsProtocolVersion)
        _ = try request(.hello, w.data)
    }

    deinit {
        Darwin.close(fd)
    }

    /// One request/response round trip. Returns the response payload past the errno; throws
    /// POSIXError when the server reports one.
    func request(_ op: VfsOp, _ payload: Data) throws -> WireReader {
        var frame = Data()
        var len = UInt32(1 + payload.count).littleEndian
        withUnsafeBytes(of: &len) { frame.append(contentsOf: $0) }
        frame.append(op.rawValue)
        frame.append(payload)
        try writeAll(frame)

        let respLen = try readU32()
        guard respLen >= 4, respLen < 8 * 1024 * 1024 else { throw POSIXError(.EIO) }
        let resp = try readExact(Int(respLen))
        var r = WireReader(resp)
        let errno = try r.i32()
        if errno != 0 {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        return r
    }

    private func writeAll(_ data: Data) throws {
        try data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) in
            var off = 0
            while off < raw.count {
                let n = Darwin.write(fd, raw.baseAddress! + off, raw.count - off)
                if n <= 0 { throw POSIXError(.EIO) }
                off += n
            }
        }
    }

    private func readU32() throws -> UInt32 {
        try readExact(4).withUnsafeBytes { $0.loadUnaligned(as: UInt32.self).littleEndian }
    }

    private func readExact(_ n: Int) throws -> Data {
        var out = Data(count: n)
        var off = 0
        try out.withUnsafeMutableBytes { (raw: UnsafeMutableRawBufferPointer) in
            while off < n {
                let got = Darwin.read(fd, raw.baseAddress! + off, n - off)
                if got <= 0 { throw POSIXError(.EIO) }
                off += got
            }
        }
        return out
    }
}

/// A small checkout pool so concurrent kernel operations don't serialize on one socket.
final class VfsPool {
    private let host: String
    private let port: UInt16
    private let secret: String
    private var idle: [VfsConnection] = []
    private var created = 0
    private let capacity = 6
    private let lock = NSCondition()

    init(host: String, port: UInt16, secret: String) {
        self.host = host
        self.port = port
        self.secret = secret
    }

    func withConnection<T>(_ body: (VfsConnection) throws -> T) throws -> T {
        let conn = try checkout()
        do {
            let out = try body(conn)
            checkin(conn)
            return out
        } catch let e as POSIXError {
            // Protocol-level errors keep the connection healthy; only transport errors (EIO)
            // drop it.
            if e.code == .EIO {
                drop()
            } else {
                checkin(conn)
            }
            throw e
        } catch {
            drop()
            throw error
        }
    }

    private func checkout() throws -> VfsConnection {
        lock.lock()
        while true {
            if let conn = idle.popLast() {
                lock.unlock()
                return conn
            }
            if created < capacity {
                created += 1
                lock.unlock()
                do {
                    return try VfsConnection(host: host, port: port, secret: secret)
                } catch {
                    lock.lock()
                    created -= 1
                    lock.unlock()
                    throw error
                }
            }
            lock.wait()
        }
    }

    private func checkin(_ conn: VfsConnection) {
        lock.lock()
        idle.append(conn)
        lock.signal()
        lock.unlock()
    }

    private func drop() {
        lock.lock()
        created -= 1
        lock.signal()
        lock.unlock()
    }
}

// MARK: - Items

final class TLFSItem: FSItem {
    let ino: UInt64
    var itemType: FSItem.ItemType
    /// Server-side lookup references owed; reclaim sends one FORGET with the total.
    var lookups: UInt64 = 0
    /// Open file handle on the daemon, when the kernel has this item open.
    var fh: UInt64?
    var fhWritable = false
    let stateLock = NSLock()

    init(ino: UInt64, type: FSItem.ItemType) {
        self.ino = ino
        self.itemType = type
        super.init()
    }
}

// MARK: - File system

@objc(TLFSFileSystem)
final class TLFSFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {

    private func endpoint(from resource: FSResource) -> (String, UInt16, String)? {
        guard let res = resource as? FSGenericURLResource,
            let url = res.url as URL?,
            url.scheme?.lowercased() == "tlfs",
            let host = url.host, let port = url.port
        else { return nil }
        let secret = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        guard !secret.isEmpty else { return nil }
        return (host, UInt16(port), secret)
    }

    func probeResource(
        resource: FSResource,
        replyHandler reply: @escaping (FSProbeResult?, (any Error)?) -> Void
    ) {
        guard let (_, port, secret) = endpoint(from: resource) else {
            reply(FSProbeResult.notRecognized, nil)
            return
        }
        // Container identity: derived from the endpoint so concurrent mounts stay distinct.
        var hasher = Hasher()
        hasher.combine(port)
        hasher.combine(secret)
        let h = UInt64(bitPattern: Int64(hasher.finalize()))
        let uuid = UUID(
            uuid: (
                UInt8(truncatingIfNeeded: h >> 56), UInt8(truncatingIfNeeded: h >> 48),
                UInt8(truncatingIfNeeded: h >> 40), UInt8(truncatingIfNeeded: h >> 32),
                UInt8(truncatingIfNeeded: h >> 24), UInt8(truncatingIfNeeded: h >> 16),
                UInt8(truncatingIfNeeded: h >> 8), UInt8(truncatingIfNeeded: h),
                0x54, 0x4C, 0x46, 0x53, // "TLFS"
                UInt8(truncatingIfNeeded: port >> 8), UInt8(truncatingIfNeeded: port),
                0x00, 0x01
            ))
        reply(FSProbeResult.usable(name: "tlfs", containerID: FSContainerIdentifier(uuid: uuid)), nil)
    }

    func loadResource(
        resource: FSResource,
        options: FSTaskOptions,
        replyHandler reply: @escaping (FSVolume?, (any Error)?) -> Void
    ) {
        guard let (host, port, secret) = endpoint(from: resource) else {
            let err = NSError(
                domain: FSKitErrorDomain, code: FSError.Code.resourceUnrecognized.rawValue)
            containerStatus = FSContainerStatus.notReady(status: err)
            reply(nil, err)
            return
        }
        let pool = VfsPool(host: host, port: port, secret: secret)
        // Validate connectivity + secret now so a bad URL fails the mount, not the first read.
        do {
            _ = try pool.withConnection { conn in
                try conn.request(.getattr, {
                    var w = WireWriter()
                    w.u64(1)
                    return w.data
                }())
            }
        } catch {
            log.error("tlfs: cannot reach mount daemon: \(error)")
            containerStatus = FSContainerStatus.notReady(status: error as NSError)
            reply(nil, error)
            return
        }
        containerStatus = .ready
        reply(TLFSVolume(pool: pool, port: port), nil)
    }

    func unloadResource(
        resource: FSResource,
        options: FSTaskOptions,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        reply(nil)
    }

    func didFinishLoading() {}
}

// MARK: - Volume

final class TLFSVolume: FSVolume {

    let pool: VfsPool
    private let itemsLock = NSLock()
    private var items: [UInt64: TLFSItem] = [:]
    let root: TLFSItem

    init(pool: VfsPool, port: UInt16) {
        self.pool = pool
        self.root = TLFSItem(ino: 1, type: .directory)
        let volUUID = UUID()
        super.init(
            volumeID: FSVolume.Identifier(uuid: volUUID),
            volumeName: FSFileName(string: "tlfs"))
        itemsLock.lock()
        items[1] = root
        itemsLock.unlock()
    }

    /// The canonical TLFSItem for an ino, creating/updating from wire attributes. Bumps the
    /// server-lookup debt when `countLookup` (reclaim repays it).
    func item(for attr: WireAttr, countLookup: Bool) -> TLFSItem {
        itemsLock.lock()
        defer { itemsLock.unlock() }
        let item: TLFSItem
        if let existing = items[attr.ino] {
            existing.itemType = attr.itemType
            item = existing
        } else {
            item = TLFSItem(ino: attr.ino, type: attr.itemType)
            items[attr.ino] = item
        }
        if countLookup {
            item.lookups += 1
        }
        return item
    }

    func dropItem(_ item: TLFSItem) {
        itemsLock.lock()
        items.removeValue(forKey: item.ino)
        itemsLock.unlock()
    }

    fileprivate func attributes(_ attr: WireAttr) -> FSItem.Attributes {
        let out = FSItem.Attributes()
        out.invalidateAllProperties()
        out.uid = getuid()
        out.gid = getgid()
        out.type = attr.itemType
        out.fileID = FSItem.Identifier(rawValue: attr.ino) ?? .invalid
        out.parentID = attr.ino == 1 ? .parentOfRoot : .rootDirectory
        out.linkCount = attr.itemType == .directory ? 2 : 1
        out.mode = UInt32(attr.perm)
        out.allocSize = (attr.size + 4095) & ~4095
        out.size = attr.size
        out.flags = 0
        var ts = timespec()
        ts.tv_sec = time(nil)
        out.accessTime = ts
        out.modifyTime = ts
        out.changeTime = ts
        out.birthTime = ts
        return out
    }

    /// Fetch wire attributes for an ino.
    fileprivate func getattr(_ ino: UInt64) throws -> WireAttr {
        try pool.withConnection { conn in
            var w = WireWriter()
            w.u64(ino)
            var r = try conn.request(.getattr, w.data)
            return try WireAttr(&r)
        }
    }

    /// Ensure the item has an open server handle with at least the requested writability.
    /// Mirrors the passthrough sample's open-mode upgrade discipline.
    fileprivate func ensureHandle(_ item: TLFSItem, write: Bool) throws -> UInt64 {
        item.stateLock.lock()
        defer { item.stateLock.unlock() }
        if let fh = item.fh, item.fhWritable || !write {
            return fh
        }
        let fh: UInt64 = try pool.withConnection { conn in
            var w = WireWriter()
            w.u64(item.ino)
            w.u8(write ? 1 : 0)
            var r = try conn.request(.open, w.data)
            return try r.u64()
        }
        if let old = item.fh {
            releaseHandle(old)
        }
        item.fh = fh
        item.fhWritable = write
        return fh
    }

    fileprivate func releaseHandle(_ fh: UInt64) {
        _ = try? pool.withConnection { conn in
            var w = WireWriter()
            w.u64(fh)
            _ = try conn.request(.release, w.data)
        }
    }
}

// MARK: - PathConf

extension TLFSVolume: FSVolume.PathConfOperations {
    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var restrictsOwnershipChanges: Bool { true }
    var truncatesLongNames: Bool { false }
    var maximumXattrSize: Int { 0 }
    var maximumFileSize: UInt64 { UInt64.max }
}

// MARK: - Core operations

extension TLFSVolume: FSVolume.Operations {

    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let caps = FSVolume.SupportedCapabilities()
        caps.supportsPersistentObjectIDs = true
        caps.supportsSymbolicLinks = true
        caps.supportsHardLinks = false
        caps.supportsJournal = false
        caps.supportsActiveJournal = false
        caps.doesNotSupportRootTimes = true
        caps.supportsSparseFiles = false
        caps.supportsZeroRuns = false
        caps.supportsFastStatFS = true
        caps.supports2TBFiles = true
        caps.supportsOpenDenyModes = false
        caps.supportsHiddenFiles = false
        caps.doesNotSupportVolumeSizes = false
        caps.supports64BitObjectIDs = true
        caps.doesNotSupportImmutableFiles = true
        caps.caseFormat = .sensitive
        return caps
    }

    var volumeStatistics: FSStatFSResult {
        let stat = FSStatFSResult(fileSystemTypeName: "tlfs")
        stat.blockSize = 4096
        stat.ioSize = 1 << 20
        var total: UInt64 = 1 << 40
        var free: UInt64 = 1 << 39
        var files: UInt64 = 1 << 20
        if let (t, f, n) = try? pool.withConnection({ conn -> (UInt64, UInt64, UInt64) in
            var r = try conn.request(.statfs, Data())
            return (try r.u64(), try r.u64(), try r.u64())
        }) {
            (total, free, files) = (t, f, n)
        }
        stat.totalBytes = total
        stat.availableBytes = free
        stat.freeBytes = free
        stat.usedBytes = total - free
        stat.totalBlocks = total / 4096
        stat.availableBlocks = free / 4096
        stat.freeBlocks = free / 4096
        stat.usedBlocks = (total - free) / 4096
        stat.totalFiles = files
        stat.freeFiles = files
        stat.fileSystemSubType = 0
        return stat
    }

    func activate(
        options: FSTaskOptions,
        replyHandler reply: @escaping (FSItem?, (any Error)?) -> Void
    ) {
        reply(root, nil)
    }

    func deactivate(
        options: FSDeactivateOptions = [],
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        reply(nil)
    }

    func mount(
        options: FSTaskOptions,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        reply(nil)
    }

    func unmount(replyHandler reply: @escaping () -> Void) {
        reply()
    }

    func synchronize(
        flags: FSSyncFlags,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        reply(nil)
    }

    func getAttributes(
        _ desiredAttributes: FSItem.GetAttributesRequest,
        of item: FSItem,
        replyHandler reply: @escaping (FSItem.Attributes?, (any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(nil, POSIXError(.EIO))
            return
        }
        do {
            let attr = try getattr(item.ino)
            reply(attributes(attr), nil)
        } catch {
            reply(nil, error)
        }
    }

    func setAttributes(
        _ newAttributes: FSItem.SetAttributesRequest,
        on item: FSItem,
        replyHandler reply: @escaping (FSItem.Attributes?, (any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(nil, POSIXError(.EIO))
            return
        }
        do {
            var w = WireWriter()
            w.u64(item.ino)
            let hasSize = newAttributes.isValid(.size)
            w.u8(hasSize ? 1 : 0)
            w.u64(hasSize ? newAttributes.size : 0)
            let hasMode = newAttributes.isValid(.mode)
            w.u8(hasMode ? 1 : 0)
            w.u32(hasMode ? newAttributes.mode : 0)
            let attr = try pool.withConnection { conn -> WireAttr in
                var r = try conn.request(.setattr, w.data)
                return try WireAttr(&r)
            }
            reply(attributes(attr), nil)
        } catch {
            reply(nil, error)
        }
    }

    func lookupItem(
        named name: FSFileName,
        inDirectory directory: FSItem,
        replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void
    ) {
        guard let dir = directory as? TLFSItem else {
            reply(nil, nil, POSIXError(.ENOTDIR))
            return
        }
        guard let nameStr = name.string else {
            reply(nil, nil, POSIXError(.ENOENT))
            return
        }
        do {
            var w = WireWriter()
            w.u64(dir.ino)
            w.str(nameStr)
            let attr = try pool.withConnection { conn -> WireAttr in
                var r = try conn.request(.lookup, w.data)
                return try WireAttr(&r)
            }
            let item = item(for: attr, countLookup: true)
            reply(item, name, nil)
        } catch {
            reply(nil, nil, error)
        }
    }

    func reclaimItem(
        _ item: FSItem,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(nil)
            return
        }
        if let fh = item.fh {
            releaseHandle(fh)
            item.fh = nil
        }
        let owed = item.lookups
        if owed > 0 {
            _ = try? pool.withConnection { conn in
                var w = WireWriter()
                w.u64(item.ino)
                w.u64(owed)
                _ = try conn.request(.forget, w.data)
            }
        }
        dropItem(item)
        reply(nil)
    }

    func readSymbolicLink(
        _ item: FSItem,
        replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(nil, POSIXError(.EIO))
            return
        }
        do {
            var w = WireWriter()
            w.u64(item.ino)
            let target = try pool.withConnection { conn -> Data in
                var r = try conn.request(.readlink, w.data)
                return try r.bytes()
            }
            reply(FSFileName(data: target), nil)
        } catch {
            reply(nil, error)
        }
    }

    func createItem(
        named name: FSFileName,
        type: FSItem.ItemType,
        inDirectory directory: FSItem,
        attributes newAttributes: FSItem.SetAttributesRequest,
        replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void
    ) {
        guard let dir = directory as? TLFSItem, let nameStr = name.string else {
            reply(nil, nil, POSIXError(.EINVAL))
            return
        }
        do {
            switch type {
            case .directory:
                var w = WireWriter()
                w.u64(dir.ino)
                w.str(nameStr)
                let attr = try pool.withConnection { conn -> WireAttr in
                    var r = try conn.request(.mkdir, w.data)
                    return try WireAttr(&r)
                }
                reply(item(for: attr, countLookup: true), name, nil)
            case .file:
                let exec = newAttributes.isValid(.mode) && (newAttributes.mode & 0o111) != 0
                var w = WireWriter()
                w.u64(dir.ino)
                w.str(nameStr)
                w.u8(exec ? 1 : 0)
                let (attr, fh) = try pool.withConnection { conn -> (WireAttr, UInt64) in
                    var r = try conn.request(.create, w.data)
                    let attr = try WireAttr(&r)
                    return (attr, try r.u64())
                }
                let created = item(for: attr, countLookup: true)
                created.stateLock.lock()
                created.fh = fh
                created.fhWritable = true
                created.stateLock.unlock()
                reply(created, name, nil)
            default:
                reply(nil, nil, POSIXError(.ENOTSUP))
            }
        } catch {
            reply(nil, nil, error)
        }
    }

    func createSymbolicLink(
        named name: FSFileName,
        inDirectory directory: FSItem,
        attributes newAttributes: FSItem.SetAttributesRequest,
        linkContents contents: FSFileName,
        replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void
    ) {
        guard let dir = directory as? TLFSItem, let nameStr = name.string,
            let target = contents.string
        else {
            reply(nil, nil, POSIXError(.EINVAL))
            return
        }
        do {
            var w = WireWriter()
            w.u64(dir.ino)
            w.str(nameStr)
            w.str(target)
            let attr = try pool.withConnection { conn -> WireAttr in
                var r = try conn.request(.symlink, w.data)
                return try WireAttr(&r)
            }
            reply(item(for: attr, countLookup: true), name, nil)
        } catch {
            reply(nil, nil, error)
        }
    }

    func createLink(
        to item: FSItem,
        named name: FSFileName,
        inDirectory directory: FSItem,
        replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void
    ) {
        reply(nil, POSIXError(.ENOTSUP))
    }

    func removeItem(
        _ item: FSItem,
        named name: FSFileName,
        fromDirectory directory: FSItem,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        guard let dir = directory as? TLFSItem, let target = item as? TLFSItem,
            let nameStr = name.string
        else {
            reply(POSIXError(.EINVAL))
            return
        }
        do {
            var w = WireWriter()
            w.u64(dir.ino)
            w.str(nameStr)
            let op: VfsOp = target.itemType == .directory ? .rmdir : .unlink
            _ = try pool.withConnection { conn in
                try conn.request(op, w.data)
            }
            reply(nil)
        } catch {
            reply(error)
        }
    }

    func renameItem(
        _ item: FSItem,
        inDirectory sourceDirectory: FSItem,
        named sourceName: FSFileName,
        to destinationName: FSFileName,
        inDirectory destinationDirectory: FSItem,
        overItem: FSItem?,
        replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void
    ) {
        guard let src = sourceDirectory as? TLFSItem, let dst = destinationDirectory as? TLFSItem,
            let srcName = sourceName.string, let dstName = destinationName.string
        else {
            reply(nil, POSIXError(.EINVAL))
            return
        }
        do {
            var w = WireWriter()
            w.u64(src.ino)
            w.str(srcName)
            w.u64(dst.ino)
            w.str(dstName)
            _ = try pool.withConnection { conn in
                try conn.request(.rename, w.data)
            }
            reply(destinationName, nil)
        } catch {
            reply(nil, error)
        }
    }

    func enumerateDirectory(
        _ directory: FSItem,
        startingAt cookie: FSDirectoryCookie,
        verifier: FSDirectoryVerifier,
        attributes: FSItem.GetAttributesRequest?,
        packer: FSDirectoryEntryPacker,
        replyHandler reply: @escaping (FSDirectoryVerifier, (any Error)?) -> Void
    ) {
        guard let dir = directory as? TLFSItem else {
            reply(FSDirectoryVerifier(rawValue: 0), POSIXError(.ENOTDIR))
            return
        }
        do {
            // Stateless per call: open, page from the cookie offset, release. The daemon's dir
            // handles snapshot the merged listing, so offsets within one call are stable.
            let fh = try pool.withConnection { conn -> UInt64 in
                var w = WireWriter()
                w.u64(dir.ino)
                var r = try conn.request(.opendir, w.data)
                return try r.u64()
            }
            defer {
                _ = try? pool.withConnection { conn in
                    var w = WireWriter()
                    w.u64(fh)
                    _ = try conn.request(.releasedir, w.data)
                }
            }
            // Cookie space: 0 = ".", 1 = "..", then daemon offsets shifted by 2.
            var cookieValue = cookie.rawValue
            if attributes != nil && cookieValue < 2 {
                cookieValue = 2
            }
            var full = false
            while cookieValue < 2 && !full {
                let (name, next) =
                    cookieValue == 0 ? (".", FSDirectoryCookie(rawValue: 1)) : ("..", FSDirectoryCookie(rawValue: 2))
                full = !packer.packEntry(
                    name: FSFileName(string: name),
                    itemType: .directory,
                    itemID: dir.ino == 1 ? .rootDirectory : (FSItem.Identifier(rawValue: dir.ino) ?? .invalid),
                    nextCookie: next,
                    attributes: nil)
                if full { break }
                cookieValue = next.rawValue
            }
            while !full {
                var w = WireWriter()
                w.u64(fh)
                w.u64(cookieValue - 2)
                w.u32(256)
                let entries = try pool.withConnection { conn -> [(UInt64, UInt8, String)] in
                    var r = try conn.request(.readdir, w.data)
                    let count = try r.u32()
                    var out: [(UInt64, UInt8, String)] = []
                    out.reserveCapacity(Int(count))
                    for _ in 0..<count {
                        let next = try r.u64()
                        let kind = try r.u8()
                        let name = try r.str()
                        out.append((next, kind, name))
                    }
                    return out
                }
                if entries.isEmpty {
                    break
                }
                for (next, kind, name) in entries {
                    let itemType: FSItem.ItemType =
                        kind == 0 ? .directory : (kind == 2 ? .symlink : .file)
                    // When per-entry attributes are requested, resolve them via lookup and
                    // immediately repay the reference.
                    var packedAttrs: FSItem.Attributes? = nil
                    var entryID = FSItem.Identifier.invalid
                    if attributes != nil {
                        var lw = WireWriter()
                        lw.u64(dir.ino)
                        lw.str(name)
                        if let attr = try? pool.withConnection({ conn -> WireAttr in
                            var r = try conn.request(.lookup, lw.data)
                            return try WireAttr(&r)
                        }) {
                            packedAttrs = self.attributes(attr)
                            entryID = FSItem.Identifier(rawValue: attr.ino) ?? .invalid
                            _ = try? pool.withConnection { conn in
                                var fw = WireWriter()
                                fw.u64(attr.ino)
                                fw.u64(1)
                                _ = try conn.request(.forget, fw.data)
                            }
                        }
                    }
                    full = !packer.packEntry(
                        name: FSFileName(string: name),
                        itemType: itemType,
                        itemID: entryID,
                        nextCookie: FSDirectoryCookie(rawValue: next + 2),
                        attributes: packedAttrs)
                    if full { break }
                    cookieValue = next + 2
                }
            }
            reply(FSDirectoryVerifier(rawValue: 0x746c6673), nil)
        } catch {
            reply(FSDirectoryVerifier(rawValue: 0), error)
        }
    }
}

// MARK: - Open/close

extension TLFSVolume: FSVolume.OpenCloseOperations {
    func openItem(
        _ item: FSItem,
        modes: FSVolume.OpenModes,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(POSIXError(.EIO))
            return
        }
        if item.itemType == .directory {
            reply(nil)
            return
        }
        do {
            _ = try ensureHandle(item, write: modes.contains(.write))
            reply(nil)
        } catch {
            reply(error)
        }
    }

    func closeItem(
        _ item: FSItem,
        modes: FSVolume.OpenModes,
        replyHandler reply: @escaping ((any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(nil)
            return
        }
        item.stateLock.lock()
        let fh = item.fh
        item.fh = nil
        item.fhWritable = false
        item.stateLock.unlock()
        if let fh {
            // Flush before release so close(2) durability expectations hold.
            _ = try? pool.withConnection { conn in
                var w = WireWriter()
                w.u64(fh)
                _ = try conn.request(.fsync, w.data)
            }
            releaseHandle(fh)
        }
        reply(nil)
    }
}

// MARK: - Read/write

extension TLFSVolume: FSVolume.ReadWriteOperations {
    func read(
        from item: FSItem,
        at offset: off_t,
        length: Int,
        into buffer: FSMutableFileDataBuffer,
        replyHandler reply: @escaping (Int, (any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(0, POSIXError(.EIO))
            return
        }
        do {
            let fh = try ensureHandle(item, write: false)
            let want = min(length, buffer.length)
            var w = WireWriter()
            w.u64(fh)
            w.u64(UInt64(max(offset, 0)))
            w.u32(UInt32(want))
            let data = try pool.withConnection { conn -> Data in
                var r = try conn.request(.read, w.data)
                return try r.bytes()
            }
            let copied = buffer.withUnsafeMutableBytes { raw -> Int in
                data.withUnsafeBytes { src -> Int in
                    let n = min(data.count, raw.count)
                    if n > 0 {
                        memcpy(raw.baseAddress!, src.baseAddress!, n)
                    }
                    return n
                }
            }
            reply(copied, nil)
        } catch {
            reply(0, error)
        }
    }

    func write(
        contents: Data,
        to item: FSItem,
        at offset: off_t,
        replyHandler reply: @escaping (Int, (any Error)?) -> Void
    ) {
        guard let item = item as? TLFSItem else {
            reply(0, POSIXError(.EIO))
            return
        }
        do {
            let fh = try ensureHandle(item, write: true)
            var total = 0
            // Chunk to stay under the protocol frame cap.
            let chunkMax = 2 * 1024 * 1024
            while total < contents.count {
                let n = min(chunkMax, contents.count - total)
                var w = WireWriter()
                w.u64(fh)
                w.u64(UInt64(max(offset, 0)) + UInt64(total))
                w.bytes(contents.subdata(in: (contents.startIndex + total)..<(contents.startIndex + total + n)))
                let wrote = try pool.withConnection { conn -> UInt32 in
                    var r = try conn.request(.write, w.data)
                    return try r.u32()
                }
                if wrote == 0 {
                    break
                }
                total += Int(wrote)
            }
            reply(total, nil)
        } catch {
            reply(0, error)
        }
    }
}
