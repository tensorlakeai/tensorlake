#!/bin/zsh
# Build + assemble + sign TLFSHello.app with embedded TLFSHelloFS.appex (FSKit module).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
export DEVELOPER_DIR=/Applications/Xcode-26.5.0.app/Contents/Developer
SDK="$(xcrun --show-sdk-path)"
TARGET=arm64-apple-macos26.0
IDENTITY="Apple Development: DIPTANU GONCHOUDHURY (F8X572HKQY)"

BUILD="$ROOT/build"
APP="$BUILD/TLFSHello.app"
APPEX="$APP/Contents/Extensions/TLFSHelloFS.appex"

rm -rf "$BUILD"
mkdir -p "$APP/Contents/MacOS" "$APPEX/Contents/MacOS"

echo "== compiling appex binary =="
xcrun swiftc \
    -target "$TARGET" -sdk "$SDK" \
    -parse-as-library -swift-version 5 -O \
    -framework FSKit -framework ExtensionFoundation -framework Foundation \
    -Xlinker -e -Xlinker _NSExtensionMain \
    "$ROOT/Sources/Extension/HelloFS.swift" \
    -o "$APPEX/Contents/MacOS/TLFSHelloFS"

echo "== compiling host app binary =="
xcrun swiftc \
    -target "$TARGET" -sdk "$SDK" \
    -framework Foundation \
    "$ROOT/Sources/App/main.swift" \
    -o "$APP/Contents/MacOS/TLFSHello"

echo "== assembling bundles =="
cp "$ROOT/Resources/App-Info.plist"   "$APP/Contents/Info.plist"
cp "$ROOT/Resources/Appex-Info.plist" "$APPEX/Contents/Info.plist"

PROFILE="${TLFS_PROVISION_PROFILE:-$HOME/Downloads/tlfsfsmoduledev.provisionprofile}"
if [[ -f "$PROFILE" ]]; then
    echo "== embedding provisioning profile ($PROFILE) =="
    cp "$PROFILE" "$APPEX/Contents/embedded.provisionprofile"
else
    echo "WARNING: provisioning profile not found at $PROFILE; appex launch will be blocked by AMFI"
fi

echo "== signing =="
codesign --force --options runtime --timestamp=none \
    --entitlements "$ROOT/Resources/Appex.entitlements" \
    --sign "$IDENTITY" "$APPEX"
codesign --force --options runtime --timestamp=none \
    --sign "$IDENTITY" "$APP"

echo "== verify =="
codesign -dv --entitlements - "$APPEX" 2>&1 | sed -n '1,40p'
echo "OK: $APP"
