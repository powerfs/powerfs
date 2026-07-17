fn main() {
    #[cfg(feature = "spdk-stub")]
    {
        cc::Build::new()
            .file("src/storage_backend/spdk_stub.c")
            .compile("spdk_stub");
    }
}
