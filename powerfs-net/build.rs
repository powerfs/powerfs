//! Build script for powerfs-net.
//!
//! When the `rdma` feature is enabled:
//! 1. Compiles `rdma_wrapper.c` — thin C wrappers for libibverbs `static
//!    inline` functions (`ibv_poll_cq`, `ibv_post_send`, `ibv_post_recv`)
//!    that are NOT exported from `libibverbs.so` and thus cannot be called
//!    via Rust FFI directly.
//! 2. Links the wrapper object + `libibverbs` + `librdmacm` into the final
//!    binary.
//!
//! Requires: `libibverbs-dev` and `librdmacm-dev` (or equivalent).

use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=rdma_wrapper.c");
    println!("cargo:rerun-if-changed=build.rs");

    if std::env::var("CARGO_FEATURE_RDMA").is_ok() {
        // --- 1. Compile the C wrapper -----------------------------------
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let wrapper_src = PathBuf::from(&manifest_dir).join("rdma_wrapper.c");
        let out_dir = std::env::var("OUT_DIR").unwrap();
        let out_obj = PathBuf::from(&out_dir).join("rdma_wrapper.o");

        let cc = std::env::var("CC").unwrap_or_else(|_| "gcc".to_string());
        let status = std::process::Command::new(&cc)
            .arg("-c")
            .arg("-fPIC")
            .arg("-O2")
            .arg("-o")
            .arg(&out_obj)
            .arg(&wrapper_src)
            .status()
            .expect("failed to compile rdma_wrapper.c — is gcc installed?");

        if !status.success() {
            panic!("gcc failed to compile rdma_wrapper.c — is libibverbs-dev installed?");
        }

        // --- 2. Link directives -----------------------------------------
        // Archive the wrapper object as a static lib so cargo can link it.
        let out_lib = PathBuf::from(&out_dir).join("libpowerfs_rdma_wrapper.a");
        let ar = std::env::var("AR").unwrap_or_else(|_| "ar".to_string());
        std::process::Command::new(&ar)
            .arg("rcs")
            .arg(&out_lib)
            .arg(&out_obj)
            .status()
            .expect("failed to archive rdma_wrapper.o");

        println!("cargo:rustc-link-search=native={}", out_dir);
        println!("cargo:rustc-link-lib=static=powerfs_rdma_wrapper");
        println!("cargo:rustc-link-lib=dylib=ibverbs");
        println!("cargo:rustc-link-lib=dylib=rdmacm");
    }
}
