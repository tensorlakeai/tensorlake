#!/bin/zsh
# Build + assemble + sign TLFS.app with embedded TLFSModule.appex (FSKit module).
#
# Modes:
#   ./build.sh                       dev build (default): Apple Development identity + a
#                                    device-limited development profile. Launches ONLY on Macs
#                                    whose UDID is in the profile — good for iteration, useless
#                                    for distribution.
#   ./build.sh --release             distribution build: Developer ID identity + a Developer ID
#                                    provisioning profile (no device list), hardened runtime,
#                                    secure timestamp. Runs on any Mac once notarized.
#   ./build.sh --release --notarize  additionally submits to Apple notary service, staples the
#                                    ticket, and emits build/TLFS.app.zip ready to distribute.
#
# Environment:
#   TLFS_IDENTITY           codesign identity. Defaults: dev -> the team's Apple Development
#                           cert; release -> "Developer ID Application" (codesign prefix match).
#   TLFS_PROVISION_PROFILE  path to the .provisionprofile embedded in the appex. Required in
#                           release mode; defaults to the dev profile path otherwise.
#   TLFS_VERSION            stamp CFBundleShortVersionString/CFBundleVersion (release pipeline
#                           passes the workspace version so app and CLI stay in lockstep).
#   Notary auth (pick one):
#     TLFS_NOTARY_PROFILE   notarytool keychain profile name (`notarytool store-credentials`)
#     TLFS_NOTARY_KEY_FILE + TLFS_NOTARY_KEY_ID + TLFS_NOTARY_ISSUER
#                           App Store Connect API key (.p8) — what CI uses.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
# Prefer the pinned Xcode when present (the FSKit surface this module uses needs the macOS 26
# SDK); otherwise trust xcode-select (CI runners).
if [[ -z "${DEVELOPER_DIR:-}" && -d /Applications/Xcode-26.5.0.app ]]; then
    export DEVELOPER_DIR=/Applications/Xcode-26.5.0.app/Contents/Developer
fi
SDK="$(xcrun --show-sdk-path)"
TARGET=arm64-apple-macos26.0

RELEASE=0
NOTARIZE=0
for arg in "$@"; do
    case "$arg" in
        --release)  RELEASE=1 ;;
        --notarize) NOTARIZE=1 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done
if (( NOTARIZE && !RELEASE )); then
    echo "--notarize requires --release (Apple rejects development-signed submissions)" >&2
    exit 2
fi

if (( RELEASE )); then
    IDENTITY="${TLFS_IDENTITY:-Developer ID Application}"
    PROFILE="${TLFS_PROVISION_PROFILE:-}"
    TIMESTAMP=(--timestamp)
    if [[ -z "$PROFILE" || ! -f "$PROFILE" ]]; then
        echo "release mode needs TLFS_PROVISION_PROFILE pointing at a Developer ID provisioning" >&2
        echo "profile for ai.tensorlake.tlfs.fsmodule with the FSKit Module capability." >&2
        exit 2
    fi
else
    IDENTITY="${TLFS_IDENTITY:-Apple Development: DIPTANU GONCHOUDHURY (F8X572HKQY)}"
    PROFILE="${TLFS_PROVISION_PROFILE:-$HOME/Downloads/tlfsfsmoduledev.provisionprofile}"
    TIMESTAMP=(--timestamp=none)
fi

BUILD="$ROOT/build"
APP="$BUILD/TLFS.app"
APPEX="$APP/Contents/Extensions/TLFSModule.appex"

rm -rf "$BUILD"
mkdir -p "$APP/Contents/MacOS" "$APPEX/Contents/MacOS"

echo "== compiling appex binary =="
xcrun swiftc \
    -target "$TARGET" -sdk "$SDK" \
    -parse-as-library -swift-version 5 -O \
    -framework FSKit -framework ExtensionFoundation -framework Foundation \
    -Xlinker -e -Xlinker _NSExtensionMain \
    "$ROOT/Sources/Extension/TLFS.swift" \
    -o "$APPEX/Contents/MacOS/TLFSModule"

echo "== compiling host app binary =="
xcrun swiftc \
    -target "$TARGET" -sdk "$SDK" \
    -framework Foundation \
    "$ROOT/Sources/App/main.swift" \
    -o "$APP/Contents/MacOS/TLFS"

echo "== assembling bundles =="
cp "$ROOT/Resources/App-Info.plist"   "$APP/Contents/Info.plist"
cp "$ROOT/Resources/Appex-Info.plist" "$APPEX/Contents/Info.plist"
if [[ -n "${TLFS_VERSION:-}" ]]; then
    for plist in "$APP/Contents/Info.plist" "$APPEX/Contents/Info.plist"; do
        plutil -replace CFBundleShortVersionString -string "$TLFS_VERSION" "$plist"
        plutil -replace CFBundleVersion -string "$TLFS_VERSION" "$plist"
    done
fi

if [[ -f "$PROFILE" ]]; then
    echo "== embedding provisioning profile ($PROFILE) =="
    cp "$PROFILE" "$APPEX/Contents/embedded.provisionprofile"
else
    echo "WARNING: provisioning profile not found at $PROFILE; appex launch will be blocked by AMFI"
fi

echo "== signing ($IDENTITY) =="
codesign --force --options runtime "${TIMESTAMP[@]}" \
    --entitlements "$ROOT/Resources/Appex.entitlements" \
    --sign "$IDENTITY" "$APPEX"
codesign --force --options runtime "${TIMESTAMP[@]}" \
    --sign "$IDENTITY" "$APP"

echo "== verify =="
codesign -dv --entitlements - "$APPEX" 2>&1 | sed -n '1,40p'

if (( NOTARIZE )); then
    echo "== notarizing =="
    notary_args=()
    if [[ -n "${TLFS_NOTARY_PROFILE:-}" ]]; then
        notary_args=(--keychain-profile "$TLFS_NOTARY_PROFILE")
    elif [[ -n "${TLFS_NOTARY_KEY_FILE:-}" ]]; then
        notary_args=(--key "$TLFS_NOTARY_KEY_FILE" --key-id "$TLFS_NOTARY_KEY_ID" --issuer "$TLFS_NOTARY_ISSUER")
    else
        echo "notarization needs TLFS_NOTARY_PROFILE or TLFS_NOTARY_KEY_FILE/_KEY_ID/_ISSUER" >&2
        exit 2
    fi
    ditto -c -k --keepParent "$APP" "$BUILD/TLFS-notarize.zip"
    xcrun notarytool submit "$BUILD/TLFS-notarize.zip" --wait "${notary_args[@]}"
    xcrun stapler staple "$APP"
    rm -f "$BUILD/TLFS-notarize.zip"
fi

if (( RELEASE )); then
    # ditto preserves signatures, extended attributes, and the stapled ticket; this zip is the
    # distribution artifact `tl fs setup` downloads and installs.
    echo "== packaging =="
    ditto -c -k --keepParent "$APP" "$BUILD/TLFS.app.zip"
    echo "OK: $BUILD/TLFS.app.zip"
fi
echo "OK: $APP"
