/* eslint-disable */
// Generated-style loader for the Tensorlake native bindings.
// Mirrors the layout produced by `@napi-rs/cli` so this package can be
// published the standard napi-rs way, but is hand-written so the file is
// readable and easy to audit.
//
// During CI the `npm/<triple>/` subpackages each contain a prebuilt
// `tensorlake-node.<triple>.node`. At install time, npm resolves only the
// platform-matching `optionalDependencies` entry; this loader requires it.
// For local development, run `npm run build` in this directory to produce
// `tensorlake-node.<triple>.node` next to this file.

const { existsSync, readFileSync } = require('fs')
const { join } = require('path')

const { platform, arch } = process

let nativeBinding = null
let localFileExisted = false
let loadError = null

function isMusl() {
  // Best-effort detection — only relevant on Linux. Other platforms always
  // report false.
  if (process.platform !== 'linux') {
    return false
  }
  let musl = false
  if (process.report && typeof process.report.getReport === 'function') {
    const { glibcVersionRuntime } = process.report.getReport().header
    musl = !glibcVersionRuntime
  } else {
    try {
      const lddPath = require('child_process').execSync('which ldd').toString().trim()
      const lddInfo = readFileSync(lddPath, 'utf8')
      musl = lddInfo.includes('musl')
    } catch (e) {
      musl = true
    }
  }
  return musl
}

const tripleMap = {
  'darwin-arm64': 'tensorlake-node.darwin-arm64.node',
  'darwin-x64': 'tensorlake-node.darwin-x64.node',
  'linux-x64-gnu': 'tensorlake-node.linux-x64-gnu.node',
  'linux-x64-musl': 'tensorlake-node.linux-x64-musl.node',
  'linux-arm64-gnu': 'tensorlake-node.linux-arm64-gnu.node',
  'linux-arm64-musl': 'tensorlake-node.linux-arm64-musl.node',
  'win32-x64-msvc': 'tensorlake-node.win32-x64-msvc.node',
}

function platformKey() {
  switch (platform) {
    case 'darwin':
      return arch === 'arm64' ? 'darwin-arm64' : 'darwin-x64'
    case 'linux': {
      const libc = isMusl() ? 'musl' : 'gnu'
      return arch === 'arm64' ? `linux-arm64-${libc}` : `linux-x64-${libc}`
    }
    case 'win32':
      return 'win32-x64-msvc'
    default:
      return null
  }
}

const key = platformKey()
const fileName = key && tripleMap[key]

if (!fileName) {
  throw new Error(`Unsupported OS/arch: ${platform}-${arch}`)
}

const localPath = join(__dirname, fileName)

if (existsSync(localPath)) {
  localFileExisted = true
  try {
    nativeBinding = require(localPath)
  } catch (e) {
    loadError = e
  }
} else {
  try {
    nativeBinding = require(`@tensorlake/native-${key}`)
  } catch (e) {
    loadError = e
  }
}

if (!nativeBinding) {
  if (loadError) {
    throw loadError
  }
  throw new Error(`Failed to load native binding for ${key}`)
}

module.exports = nativeBinding
