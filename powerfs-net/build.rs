//! Build script for powerfs-net.
//!
//! When the `rdma` feature is enabled, links against system-installed
//! `libibverbs` and `librdmacm` shared libraries. These provide the
//! FFI symbols declared in `src/transport_rdma.rs`.
//!
//! Requires: `libibverbs-dev` and `librdmacm-dev` (or equivalent) to be
//! installed on the system.

fn main() {
    // Only link RDMA libraries when the `rdma` feature is enabled.
    if std::env::var("CARGO_FEATURE_RDMA").is_ok() {
        // Link against system shared libraries. On Debian/Ubuntu these
        // are in the standard linker search path (/usr/lib/x86_64-linux-gnu).
        println!("cargo:rustc-link-lib=dylib=ibverbs");
        println!("cargo:rustc-link-lib=dylib=rdmacm");
        println!("cargo:rerun-if-changed=build.rs");
    }
}
