//! Embeds the notarized TLFS.app.zip (the macOS FSKit extension) into the binary when
//! `TLFS_APP_ZIP` points at one at build time, so `tl fs setup` installs offline with the app
//! and CLI in guaranteed lockstep. Official darwin release builds set it (publish_cli.yaml);
//! source builds don't, and setup falls back to downloading the matching release asset.

use std::env;
use std::path::Path;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(tlfs_app_embedded)");
    println!("cargo::rerun-if-env-changed=TLFS_APP_ZIP");
    if let Ok(zip) = env::var("TLFS_APP_ZIP") {
        let path = Path::new(&zip);
        assert!(
            path.is_absolute() && path.is_file(),
            "TLFS_APP_ZIP must be an absolute path to an existing TLFS.app.zip (got {zip:?})"
        );
        println!("cargo::rerun-if-changed={zip}");
        println!("cargo::rustc-cfg=tlfs_app_embedded");
        println!("cargo::rustc-env=TLFS_APP_ZIP={zip}");
    }
}
