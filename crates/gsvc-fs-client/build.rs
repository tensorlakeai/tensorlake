//! Embeds the notarized FSKit application into official macOS CLI builds.

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
