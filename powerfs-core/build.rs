fn main() {
    #[cfg(feature = "spdk-stub")]
    {
        cc::Build::new()
            .file("src/storage_backend/spdk_stub.c")
            .compile("spdk_stub");
    }

    #[cfg(feature = "spdk")]
    {
        println!("cargo:rustc-link-search=/usr/lib/x86_64-linux-gnu");
        println!("cargo:rustc-link-search=/usr/local/lib");
        
        pkg_config::Config::new()
            .probe("spdk_init")
            .expect("Failed to find spdk_init via pkg-config");
        
        pkg_config::Config::new()
            .probe("spdk_event")
            .expect("Failed to find spdk_event via pkg-config");
        
        pkg_config::Config::new()
            .probe("spdk_bdev_nvme")
            .expect("Failed to find spdk_bdev_nvme via pkg-config");
        
        pkg_config::Config::new()
            .probe("spdk_env_dpdk")
            .expect("Failed to find spdk_env_dpdk via pkg-config");
        
        pkg_config::Config::new()
            .probe("spdk_dpdklibs")
            .expect("Failed to find spdk_dpdklibs via pkg-config");
        
        pkg_config::Config::new()
            .probe("spdk_syslibs")
            .expect("Failed to find spdk_syslibs via pkg-config");

        println!("cargo:rustc-link-lib=dylib=spdk_event");
        println!("cargo:rustc-link-lib=dylib=spdk_bdev");
        println!("cargo:rustc-link-lib=dylib=spdk_nvme");
        println!("cargo:rustc-link-lib=dylib=spdk_thread");
        println!("cargo:rustc-link-lib=dylib=crypto");
        println!("cargo:rustc-link-lib=dylib=ssl");
        println!("cargo:rustc-link-lib=dylib=isal");
        println!("cargo:rustc-link-lib=dylib=isal_crypto");
        println!("cargo:rustc-link-lib=dylib=numa");
        println!("cargo:rustc-link-lib=dylib=z");
        println!("cargo:rustc-link-lib=dylib=rt");
        println!("cargo:rustc-link-lib=dylib=pthread");
        println!("cargo:rustc-link-lib=dylib=dl");
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
}
