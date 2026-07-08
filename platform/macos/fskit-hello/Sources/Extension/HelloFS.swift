//
// TLFSHelloFS — minimal FSKit FSUnaryFileSystem serving a read-only volume
// with a single file: /hello.txt  ("hello from tlfs\n")
//
import Foundation
import ExtensionFoundation
import FSKit

// MARK: - Entry point
//
// Mirrors Apple's shipping FSKit modules (msdos/exfat/ftp): the appex binary's
// entry point is Foundation's NSExtensionMain (linked via `-e _NSExtensionMain`),
// and the system instantiates EXExtensionPrincipalClass from the Info.plist.
// (The Swift @main + UnaryFileSystemExtension path launches but the process
// exits before fskit_agent can fetch its listener endpoint; see notes.)

// MARK: - Unary file system

@objc(TLFSHelloFileSystem)
final class TLFSHelloFileSystem: FSUnaryFileSystem, FSUnaryFileSystemOperations {

    private func isOurs(_ resource: FSResource) -> Bool {
        guard let res = resource as? FSGenericURLResource else { return false }
        return res.url.scheme?.lowercased() == "tlfshello"
    }

    func probeResource(resource: FSResource,
                       replyHandler reply: @escaping (FSProbeResult?, (any Error)?) -> Void) {
        guard isOurs(resource) else {
            reply(FSProbeResult.notRecognized, nil)
            return
        }
        reply(FSProbeResult.usable(name: "tlfshello",
                                   containerID: FSContainerIdentifier(uuid: TLFSHelloVolume.containerUUID)),
              nil)
    }

    func loadResource(resource: FSResource,
                      options: FSTaskOptions,
                      replyHandler reply: @escaping (FSVolume?, (any Error)?) -> Void) {
        guard isOurs(resource) else {
            let err = NSError(domain: FSKitErrorDomain,
                              code: FSError.Code.resourceUnrecognized.rawValue)
            containerStatus = FSContainerStatus.notReady(status: err)
            reply(nil, err)
            return
        }
        containerStatus = .ready
        reply(TLFSHelloVolume(), nil)
    }

    func unloadResource(resource: FSResource,
                        options: FSTaskOptions,
                        replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func didFinishLoading() {
        // no-op
    }
}

// MARK: - Items

final class HelloItem: FSItem {
    let itemID: FSItem.Identifier
    let itemType: FSItem.ItemType
    let name: String

    init(id: FSItem.Identifier, type: FSItem.ItemType, name: String) {
        self.itemID = id
        self.itemType = type
        self.name = name
        super.init()
    }
}

// MARK: - Volume

final class TLFSHelloVolume: FSVolume {

    static let containerUUID = UUID(uuidString: "8F5A2C64-9D11-4B4E-9C51-1D0C2A5A70F1")!
    static let helloData = Data("hello from tlfs\n".utf8)
    static let helloFileID = FSItem.Identifier(rawValue: 3)!

    let root = HelloItem(id: .rootDirectory, type: .directory, name: "/")
    let hello = HelloItem(id: TLFSHelloVolume.helloFileID, type: .file, name: "hello.txt")
    private let epoch: timespec

    init() {
        var ts = timespec()
        ts.tv_sec = time(nil)
        epoch = ts
        super.init(volumeID: FSVolume.Identifier(uuid: Self.containerUUID),
                   volumeName: FSFileName(string: "tlfshello"))
    }

    fileprivate func attributes(for item: HelloItem,
                                request: FSItem.GetAttributesRequest?) -> FSItem.Attributes {
        let attrs = FSItem.Attributes()
        attrs.invalidateAllProperties()
        attrs.uid = getuid()
        attrs.gid = getgid()
        attrs.type = item.itemType
        attrs.fileID = item.itemID
        attrs.parentID = item.itemID == .rootDirectory ? .parentOfRoot : .rootDirectory
        attrs.linkCount = item.itemType == .directory ? 2 : 1
        attrs.mode = item.itemType == .directory ? 0o555 : 0o444
        attrs.allocSize = 4096
        attrs.size = item.itemType == .directory ? 4096 : UInt64(Self.helloData.count)
        attrs.flags = 0
        attrs.accessTime = epoch
        attrs.modifyTime = epoch
        attrs.changeTime = epoch
        attrs.birthTime = epoch
        attrs.addedTime = epoch
        attrs.backupTime = epoch
        return attrs
    }

    private func posixError(_ code: Int32) -> NSError {
        NSError(domain: NSPOSIXErrorDomain, code: Int(code))
    }
}

// MARK: - PathConf

extension TLFSHelloVolume: FSVolume.PathConfOperations {
    var maximumLinkCount: Int { 1 }
    var maximumNameLength: Int { 255 }
    var restrictsOwnershipChanges: Bool { false }
    var truncatesLongNames: Bool { false }
    var maximumXattrSize: Int { 0 }
    var maximumFileSize: UInt64 { UInt64(Self.helloData.count) }
}

// MARK: - Core operations

extension TLFSHelloVolume: FSVolume.Operations {

    var supportedVolumeCapabilities: FSVolume.SupportedCapabilities {
        let caps = FSVolume.SupportedCapabilities()
        caps.supportsPersistentObjectIDs = true
        caps.supportsSymbolicLinks = false
        caps.supportsHardLinks = false
        caps.supportsJournal = false
        caps.supportsActiveJournal = false
        caps.doesNotSupportRootTimes = false
        caps.supportsSparseFiles = false
        caps.supportsZeroRuns = false
        caps.supportsFastStatFS = true
        caps.supports2TBFiles = false
        caps.supportsOpenDenyModes = false
        caps.supportsHiddenFiles = false
        caps.doesNotSupportVolumeSizes = false
        caps.supports64BitObjectIDs = true
        caps.doesNotSupportImmutableFiles = true
        caps.doesNotSupportSettingFilePermissions = true
        caps.caseFormat = .sensitive
        return caps
    }

    var volumeStatistics: FSStatFSResult {
        let stat = FSStatFSResult(fileSystemTypeName: "tlfshello")
        stat.blockSize = 4096
        stat.ioSize = 4096
        stat.totalBlocks = 1
        stat.availableBlocks = 0
        stat.freeBlocks = 0
        stat.usedBlocks = 1
        stat.totalBytes = UInt64(Self.helloData.count)
        stat.availableBytes = 0
        stat.freeBytes = 0
        stat.usedBytes = UInt64(Self.helloData.count)
        stat.totalFiles = 1
        stat.freeFiles = 0
        stat.fileSystemSubType = 0
        return stat
    }

    func activate(options: FSTaskOptions,
                  replyHandler reply: @escaping (FSItem?, (any Error)?) -> Void) {
        reply(root, nil)
    }

    func deactivate(options: FSDeactivateOptions = [],
                    replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func mount(options: FSTaskOptions,
               replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func unmount(replyHandler reply: @escaping () -> Void) {
        reply()
    }

    func synchronize(flags: FSSyncFlags,
                     replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func getAttributes(_ desiredAttributes: FSItem.GetAttributesRequest,
                       of item: FSItem,
                       replyHandler reply: @escaping (FSItem.Attributes?, (any Error)?) -> Void) {
        guard let item = item as? HelloItem else {
            reply(nil, posixError(EIO))
            return
        }
        reply(attributes(for: item, request: desiredAttributes), nil)
    }

    func setAttributes(_ newAttributes: FSItem.SetAttributesRequest,
                       on item: FSItem,
                       replyHandler reply: @escaping (FSItem.Attributes?, (any Error)?) -> Void) {
        reply(nil, posixError(EROFS))
    }

    func lookupItem(named name: FSFileName,
                    inDirectory directory: FSItem,
                    replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void) {
        guard let dir = directory as? HelloItem, dir.itemID == .rootDirectory else {
            reply(nil, nil, posixError(ENOTDIR))
            return
        }
        switch name.string {
        case "hello.txt":
            reply(hello, FSFileName(string: hello.name), nil)
        case ".":
            reply(root, FSFileName(string: "."), nil)
        default:
            reply(nil, nil, posixError(ENOENT))
        }
    }

    func reclaimItem(_ item: FSItem,
                     replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func readSymbolicLink(_ item: FSItem,
                          replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void) {
        reply(nil, posixError(EINVAL))
    }

    func createItem(named name: FSFileName,
                    type: FSItem.ItemType,
                    inDirectory directory: FSItem,
                    attributes newAttributes: FSItem.SetAttributesRequest,
                    replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void) {
        reply(nil, nil, posixError(EROFS))
    }

    func createSymbolicLink(named name: FSFileName,
                            inDirectory directory: FSItem,
                            attributes newAttributes: FSItem.SetAttributesRequest,
                            linkContents contents: FSFileName,
                            replyHandler reply: @escaping (FSItem?, FSFileName?, (any Error)?) -> Void) {
        reply(nil, nil, posixError(EROFS))
    }

    func createLink(to item: FSItem,
                    named name: FSFileName,
                    inDirectory directory: FSItem,
                    replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void) {
        reply(nil, posixError(EROFS))
    }

    func removeItem(_ item: FSItem,
                    named name: FSFileName,
                    fromDirectory directory: FSItem,
                    replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(posixError(EROFS))
    }

    func renameItem(_ item: FSItem,
                    inDirectory sourceDirectory: FSItem,
                    named sourceName: FSFileName,
                    to destinationName: FSFileName,
                    inDirectory destinationDirectory: FSItem,
                    overItem: FSItem?,
                    replyHandler reply: @escaping (FSFileName?, (any Error)?) -> Void) {
        reply(nil, posixError(EROFS))
    }

    func enumerateDirectory(_ directory: FSItem,
                            startingAt cookie: FSDirectoryCookie,
                            verifier: FSDirectoryVerifier,
                            attributes: FSItem.GetAttributesRequest?,
                            packer: FSDirectoryEntryPacker,
                            replyHandler reply: @escaping (FSDirectoryVerifier, (any Error)?) -> Void) {
        guard let dir = directory as? HelloItem, dir.itemID == .rootDirectory else {
            reply(FSDirectoryVerifier(rawValue: 0), posixError(ENOTDIR))
            return
        }

        // Entry index space: 0 = ".", 1 = "..", 2 = "hello.txt", 3 = end.
        // When attributes are requested, "." and ".." must not be packed.
        let verifierValue = FSDirectoryVerifier(rawValue: 0x7175)
        var index = cookie.rawValue
        if attributes != nil && index < 2 {
            index = 2
        }

        while index < 3 {
            let packed: Bool
            switch index {
            case 0:
                packed = packer.packEntry(name: FSFileName(string: "."),
                                          itemType: .directory,
                                          itemID: root.itemID,
                                          nextCookie: FSDirectoryCookie(rawValue: 1),
                                          attributes: nil)
            case 1:
                packed = packer.packEntry(name: FSFileName(string: ".."),
                                          itemType: .directory,
                                          itemID: root.itemID,
                                          nextCookie: FSDirectoryCookie(rawValue: 2),
                                          attributes: nil)
            default:
                packed = packer.packEntry(name: FSFileName(string: hello.name),
                                          itemType: .file,
                                          itemID: hello.itemID,
                                          nextCookie: FSDirectoryCookie(rawValue: 3),
                                          attributes: attributes.map { self.attributes(for: hello, request: $0) })
            }
            if !packed { break }
            index += 1
        }
        reply(verifierValue, nil)
    }
}

// MARK: - Open/close

extension TLFSHelloVolume: FSVolume.OpenCloseOperations {
    func openItem(_ item: FSItem,
                  modes: FSVolume.OpenModes,
                  replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }

    func closeItem(_ item: FSItem,
                   modes: FSVolume.OpenModes,
                   replyHandler reply: @escaping ((any Error)?) -> Void) {
        reply(nil)
    }
}

// MARK: - Read/write

extension TLFSHelloVolume: FSVolume.ReadWriteOperations {
    func read(from item: FSItem,
              at offset: off_t,
              length: Int,
              into buffer: FSMutableFileDataBuffer,
              replyHandler reply: @escaping (Int, (any Error)?) -> Void) {
        guard let item = item as? HelloItem, item.itemType == .file else {
            reply(0, posixError(EISDIR))
            return
        }
        let data = Self.helloData
        guard offset >= 0, offset < off_t(data.count) else {
            reply(0, nil)
            return
        }
        let start = Int(offset)
        let count = min(length, buffer.length, data.count - start)
        let copied = buffer.withUnsafeMutableBytes { raw -> Int in
            data.withUnsafeBytes { src -> Int in
                guard let dstBase = raw.baseAddress, let srcBase = src.baseAddress else { return 0 }
                memcpy(dstBase, srcBase + start, count)
                return count
            }
        }
        reply(copied, nil)
    }

    func write(contents: Data,
               to item: FSItem,
               at offset: off_t,
               replyHandler reply: @escaping (Int, (any Error)?) -> Void) {
        reply(0, posixError(EROFS))
    }
}
