// Minimal host app binary for the TLFSHelloFS app extension.
// It only needs to exist so the .appex has a valid host bundle.
import Foundation

FileHandle.standardOutput.write(Data("TLFSHello host app running; ^C to exit\n".utf8))
dispatchMain()
