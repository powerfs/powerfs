#[cfg(feature = "spdk")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::ffi::{CStr, CString};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    println!("=== Simple SPDK Test ===");

    static INIT_DONE: AtomicBool = AtomicBool::new(false);

    extern "C" fn spdk_start_fn(ctx: *mut std::os::raw::c_void) {
        let init_done = unsafe { &*(ctx as *const AtomicBool) };

        println!("SPDK app started on reactor thread");

        let bdevs = list_bdevs();
        println!("Available bdevs: {:?}", bdevs);

        init_done.store(true, Ordering::SeqCst);
    }

    fn list_bdevs() -> Vec<String> {
        let mut bdevs = Vec::new();
        unsafe {
            let mut bdev = spdk_bdev_first();
            while !bdev.is_null() {
                let name_ptr = spdk_bdev_get_name(bdev);
                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();
                    bdevs.push(name);
                }
                bdev = spdk_bdev_next(bdev);
            }
        }
        bdevs
    }

    extern "C" {
        fn spdk_app_start(
            opts: *const std::os::raw::c_void,
            start_fn: *const std::os::raw::c_void,
            ctx: *mut std::os::raw::c_void,
        ) -> i32;
        fn spdk_app_fini();
        fn spdk_bdev_first() -> *const std::os::raw::c_void;
        fn spdk_bdev_next(bdev: *const std::os::raw::c_void) -> *const std::os::raw::c_void;
        fn spdk_bdev_get_name(bdev: *const std::os::raw::c_void) -> *const std::os::raw::c_char;
    }

    let init_done_ptr = &INIT_DONE as *const AtomicBool as *mut std::os::raw::c_void;

    let opts: *const std::os::raw::c_void = std::ptr::null();
    let start_fn = spdk_start_fn as *const std::os::raw::c_void;

    let ret = unsafe { spdk_app_start(opts, start_fn, init_done_ptr) };
    if ret != 0 {
        return Err(format!("spdk_app_start failed: {}", ret).into());
    }

    println!("SPDK app start returned");

    unsafe {
        spdk_app_fini();
    }

    Ok(())
}

#[cfg(not(feature = "spdk"))]
fn main() {
    println!("SPDK feature not enabled");
}
