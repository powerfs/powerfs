use std::sync::{Mutex, OnceLock};
use sysinfo::{Disks, Networks, System};

#[derive(Debug, Clone)]
pub struct SystemMetrics {
    pub cpu_usage: f64,
    pub mem_usage: f64,
    pub disk_usage: f64,
    pub network_rx: u64,
    pub network_tx: u64,
    pub uptime: u64,
}

/// 缓存的磁盘列表: 首次构建时扫描 /sys/class/block + mountinfo,
/// 之后只刷新已列磁盘的用量 (statvfs), 避免每轮 metrics 重建.
fn cached_disks() -> &'static Mutex<Disks> {
    static DISKS: OnceLock<Mutex<Disks>> = OnceLock::new();
    DISKS.get_or_init(|| Mutex::new(Disks::new_with_refreshed_list()))
}

/// 缓存的网卡列表: 首次构建时扫描 /sys/class/net,
/// 之后只刷新计数器 (/proc/net/dev), 避免每轮 metrics 重建.
fn cached_networks() -> &'static Mutex<Networks> {
    static NETWORKS: OnceLock<Mutex<Networks>> = OnceLock::new();
    NETWORKS.get_or_init(|| Mutex::new(Networks::new_with_refreshed_list()))
}

pub fn collect_system_metrics(sys: &mut System, _data_dir: &str) -> SystemMetrics {
    // 只刷新 CPU 使用率与内存, 禁止 refresh_all():
    // refresh_all() 会 (1) 用 rayon 并行扫描宿主机 /proc 下所有任务做
    // 进程级刷新 (容器内看到的是宿主全部进程), (2) 刷新 CPU 频率 ——
    // 容器内没有 cpufreq sysfs 时 sysinfo 回退读 /proc/cpuinfo 的
    // "cpu MHz" 行, 内核对每个 CPU 执行 aperfmperf 快照 IPI.
    // 在大核数宿主机上, 每 5s 一次的 refresh_all() 会持续消耗数秒 CPU
    // 并引发跨核 IPI / 内核锁竞争, 与数据写路径争抢 CPU, 使
    // WriteNeedle 请求延迟恶化到秒级. 本函数只需 cpu/mem/disk/net
    // 聚合指标, 不需要任何进程级信息.
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let cpu_usage = if let Some(cpu) = sys.cpus().first() {
        cpu.cpu_usage() as f64
    } else {
        0.0
    };

    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let mem_usage = if total_memory > 0 {
        (used_memory as f64 / total_memory as f64) * 100.0
    } else {
        0.0
    };

    let mut disk_usage = 0.0;
    {
        let mut disks = cached_disks().lock().unwrap();
        disks.refresh();
        for disk in disks.list() {
            let total_space = disk.total_space();
            let available_space = disk.available_space();
            let used_space = total_space.saturating_sub(available_space);
            if total_space > 0 {
                disk_usage = (used_space as f64 / total_space as f64) * 100.0;
                break;
            }
        }
    }

    let mut network_rx = 0;
    let mut network_tx = 0;
    {
        let mut networks = cached_networks().lock().unwrap();
        networks.refresh();
        for network in networks.list().values() {
            network_rx += network.received();
            network_tx += network.transmitted();
        }
    }

    let uptime = System::uptime();

    SystemMetrics {
        cpu_usage,
        mem_usage,
        disk_usage,
        network_rx,
        network_tx,
        uptime,
    }
}
