//! Dynamic log level control for runtime debugging.
//!
//! Usage:
//! 1. In `main.rs`: initialize `env_logger` with `Debug` level (max verbosity),
//!    then call [`set_log_level`] with the configured level to gate output.
//! 2. Expose HTTP endpoints (e.g. `/admin/log-level`) in each service's metrics
//!    module that call [`set_log_level`] / [`get_log_level`].
//!
//! This works because the `log` crate's macros check `log::max_level()` before
//! dispatching to the underlying logger.  `env_logger` initialized at `Debug`
//! passes everything through its internal filter, so `log::set_max_level()` is
//! the sole gatekeeper.

use log::LevelFilter;
use std::sync::atomic::{AtomicU8, Ordering};

/// Current effective log level stored as `LevelFilter as u8`.
static CURRENT_LEVEL: AtomicU8 = AtomicU8::new(LevelFilter::Info as u8);

fn parse_level(s: &str) -> Result<LevelFilter, String> {
    match s.to_ascii_lowercase().as_str() {
        "off" => Ok(LevelFilter::Off),
        "error" => Ok(LevelFilter::Error),
        "warn" => Ok(LevelFilter::Warn),
        "info" => Ok(LevelFilter::Info),
        "debug" => Ok(LevelFilter::Debug),
        "trace" => Ok(LevelFilter::Trace),
        other => Err(format!(
            "unknown log level '{}', expected: off|error|warn|info|debug|trace",
            other
        )),
    }
}

fn level_to_str(lf: LevelFilter) -> &'static str {
    match lf {
        LevelFilter::Off => "off",
        LevelFilter::Error => "error",
        LevelFilter::Warn => "warn",
        LevelFilter::Info => "info",
        LevelFilter::Debug => "debug",
        LevelFilter::Trace => "trace",
    }
}

/// Set the runtime log level.
///
/// Called during init (after `env_logger::init()`) and from HTTP endpoints.
pub fn set_log_level(level_str: &str) -> Result<(), String> {
    let lf = parse_level(level_str)?;
    CURRENT_LEVEL.store(lf as u8, Ordering::Relaxed);
    log::set_max_level(lf);
    Ok(())
}

/// Get the current effective log level as a string.
pub fn get_log_level() -> &'static str {
    let v = CURRENT_LEVEL.load(Ordering::Relaxed);
    // LevelFilter is #[repr(u8)] with Off=0,Error=1,Warn=2,Info=3,Debug=4,Trace=5
    match v {
        0 => "off",
        1 => "error",
        2 => "warn",
        3 => "info",
        4 => "debug",
        5 => "trace",
        _ => "info",
    }
}
