pub mod powerfs {
    #![allow(clippy::result_large_err)]
    include!(concat!(env!("OUT_DIR"), "/volume_proto/powerfs.rs"));
}
