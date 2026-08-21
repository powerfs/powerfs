//! Dynamic log level control + subsystem debug flags for runtime debugging.
//!
//! ## 全局日志级别
//!
//! 1. 在 `main.rs` 中以 `Debug` 级别初始化 `env_logger`（最大冗余），
//!    然后调用 [`set_log_level`] 设置初始级别来 gate 输出。
//! 2. 通过 HTTP 端点（如 `/admin/log-level`）运行时调整。
//!
//! 原理：`log` crate 的宏在 dispatch 前检查 `log::max_level()`。
//! `env_logger` 初始化为 `Debug` 后内部 filter 放行所有日志，
//! 所以 `log::set_max_level()` 是唯一的 gatekeeper。
//!
//! ## 按 target 过滤
//!
//! [`set_target_filter`] 可以只看特定模块的日志，例如：
//! `set_target_filter("powerfs_fuse::fuse")` 只输出 fuse 模块的日志，
//! 其他模块被 `Off` 过滤。
//!
//! 实现方式：自定义 `log::Log` wrapper，在 `enabled` / `log` 方法中
//! 按 target 前缀匹配。`env_logger` 作为底层 writer。
//!
//! ## 子系统调试开关
//!
//! [`set_flag`] / [`flag`] 提供命名布尔开关，用于精确控制特定调试日志：
//! ```ignore
//! if powerfs_common::dynamic_log::flag("fuse_create_timing") {
//!     info!("create timing: ...");
//! }
//! ```
//! 通过 HTTP 端点（如 `/debug/flags?name=fuse_create_timing&on=true`）控制。
//! 预设开关见 [`DEFAULT_FLAGS`]。

use dashmap::DashMap;
use log::{LevelFilter, Metadata, Record};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

/// Current effective log level stored as `LevelFilter as u8`.
static CURRENT_LEVEL: AtomicU8 = AtomicU8::new(LevelFilter::Info as u8);

/// Target filter: when set (Some), only logs whose `target` starts with
/// this prefix are emitted. Empty string = no filter (all targets).
static TARGET_FILTER: std::sync::RwLock<Option<String>> = std::sync::RwLock::new(None);

/// Subsystem debug flags: name → enabled.
static DEBUG_FLAGS: OnceLock<DashMap<String, Arc<AtomicBool>>> = OnceLock::new();

fn flags() -> &'static DashMap<String, Arc<AtomicBool>> {
    DEBUG_FLAGS.get_or_init(|| {
        let map = DashMap::new();
        for name in DEFAULT_FLAGS {
            map.insert(name.to_string(), Arc::new(AtomicBool::new(false)));
        }
        map
    })
}

/// 预设子系统调试开关名称。
///
/// 通过 `set_flag("xxx", true)` 开启，代码中用 `flag("xxx")` 检查。
pub const DEFAULT_FLAGS: &[&str] = &[
    // FUSE 客户端
    "fuse_create_timing",    // create 路径耗时分解
    "fuse_lease_trace",      // 目录/inode lease 获取、释放、检查
    "fuse_invalidate_trace", // cache invalidate 全链路
    "fuse_open_trace",       // open 路径 cache hit/miss + RPC
    "fuse_unlink_trace",     // unlink 路径 + batch flush
    "fuse_batch_trace",      // batch_unlink/batch_create 批量提交
    "fuse_lookup_trace",     // lookup 路径（含 lease 跳过）
    "fuse_writeback_trace",  // write/release 同步路径
    // Filer 服务端
    "filer_raft_trace",     // Raft propose/commit/apply
    "filer_create_trace",   // handle_create 全流程
    "filer_redirect_trace", // leader 重定向
    "filer_lease_trace",    // 服务端 lease 管理
    // Master
    "master_topology_trace", // 拓扑/路由变更
    // 通用
    "rpc_trace",     // RPC 发送/接收/重试
    "routing_trace", // shard 路由计算
];

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

// ---------------------------------------------------------------------------
// 全局日志级别
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 按 target 过滤
// ---------------------------------------------------------------------------

/// 设置 target 过滤：只输出 `target` 以 `filter` 开头的日志。
///
/// 传入空字符串或 "all" 清除过滤（输出所有 target）。
///
/// 示例：
/// - `set_target_filter("powerfs_fuse::fuse")` — 只看 fuse 模块
/// - `set_target_filter("powerfs_filer")` — 只看 filer 模块
/// - `set_target_filter("")` — 清除过滤
pub fn set_target_filter(filter: &str) {
    let trimmed = filter.trim();
    let filter = if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("all") {
        None
    } else {
        Some(trimmed.to_string())
    };
    *TARGET_FILTER.write().unwrap() = filter;
    // 安装自定义 logger wrapper（如果尚未安装）
    install_wrapper();
}

/// 获取当前 target 过滤器。`None` 表示无过滤。
pub fn get_target_filter() -> Option<String> {
    TARGET_FILTER.read().unwrap().clone()
}

// ---------------------------------------------------------------------------
// 子系统调试开关
// ---------------------------------------------------------------------------

/// 开启或关闭一个子系统调试开关。
///
/// 如果 `name` 不在预设列表中，会自动创建。
pub fn set_flag(name: &str, on: bool) {
    if let Some(entry) = flags().get(name) {
        entry.store(on, Ordering::Relaxed);
    } else {
        flags().insert(name.to_string(), Arc::new(AtomicBool::new(on)));
    }
}

/// 检查一个子系统调试开关是否开启。
///
/// 未注册的开关返回 `false`。
///
/// 用法：
/// ```ignore
/// if powerfs_common::dynamic_log::flag("fuse_create_timing") {
///     info!("create timing: ...");
/// }
/// ```
pub fn flag(name: &str) -> bool {
    flags()
        .get(name)
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(false)
}

/// 列出所有已注册的开关及其当前状态。
///
/// 返回 `Vec<(name, enabled)>`，按名称排序。
pub fn list_flags() -> Vec<(String, bool)> {
    let mut result: Vec<(String, bool)> = flags()
        .iter()
        .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
        .collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

/// 获取所有预设开关名称（`DEFAULT_FLAGS`）。
pub fn known_flag_names() -> &'static [&'static str] {
    DEFAULT_FLAGS
}

// ---------------------------------------------------------------------------
// Logger wrapper: env_logger + target filter
// ---------------------------------------------------------------------------

/// 自定义 logger：包装 env_logger，在 log() 前按 target 过滤。
///
/// `log` crate 的 `set_logger` 只能调一次，所以用 `OnceLock` 保护。
struct TargetFilterLogger {
    env_logger: env_logger::Logger,
}

static WRAPPER_LOGGER: OnceLock<TargetFilterLogger> = OnceLock::new();
static INSTALLED: AtomicBool = AtomicBool::new(false);

fn install_wrapper() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return; // 已经安装
    }
    // 用 Debug 级别创建 env_logger（放行所有），target 过滤由 wrapper 做
    let env_logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug"))
            .format(|buf, record| {
                use std::io::Write;
                writeln!(
                    buf,
                    "[{}] [{}] [{}] {}",
                    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                    record.level(),
                    record.target(),
                    record.args()
                )
            })
            .build();

    let wrapper = TargetFilterLogger { env_logger };
    let _ = WRAPPER_LOGGER.set(wrapper);

    // 安装为全局 logger
    // OnceLock 保证 wrapper 存活到进程结束，因此引用是 'static 的。
    // INSTALLED AtomicBool 保证只调一次 set_logger。
    let logger_ref: &'static TargetFilterLogger = WRAPPER_LOGGER.get().unwrap();
    log::set_logger(logger_ref as &'static dyn log::Log).expect("set_logger failed");
    log::set_max_level(LevelFilter::Debug);
}

impl log::Log for TargetFilterLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // 先检查全局级别
        let global_level = CURRENT_LEVEL.load(Ordering::Relaxed);
        let global_filter = match global_level {
            0 => LevelFilter::Off,
            1 => LevelFilter::Error,
            2 => LevelFilter::Warn,
            3 => LevelFilter::Info,
            4 => LevelFilter::Debug,
            5 => LevelFilter::Trace,
            _ => LevelFilter::Info,
        };
        // LevelFilter -> Level: Off maps to None (filter everything)
        if let Some(global_level) = global_filter.to_level() {
            if metadata.level() > global_level {
                return false;
            }
        } else {
            // Off
            return false;
        }
        // 再检查 target 过滤
        if let Some(filter) = TARGET_FILTER.read().unwrap().as_ref() {
            if !metadata.target().starts_with(filter.as_str()) {
                return false;
            }
        }
        true
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        self.env_logger.log(record);
    }

    fn flush(&self) {
        self.env_logger.flush();
    }
}

/// 初始化动态日志系统。
///
/// 在 `main.rs` 中调用，替代直接 `env_logger::init()`。
/// - `initial_level`: 初始全局级别（如 "info"）
/// - `target_filter`: 可选 target 过滤（如 `Some("powerfs_fuse::fuse")`），`None` 不过滤
///
/// 内部安装 TargetFilterLogger 作为全局 logger。
/// env_logger 以 Debug 级别创建，`set_max_level` 作为 gatekeeper。
pub fn init(initial_level: &str, target_filter: Option<&str>) {
    install_wrapper();
    let _ = set_log_level(initial_level);
    if let Some(f) = target_filter {
        set_target_filter(f);
    }
}
