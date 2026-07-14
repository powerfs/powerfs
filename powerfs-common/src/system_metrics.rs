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

pub fn collect_system_metrics(sys: &mut System, _data_dir: &str) -> SystemMetrics {
    sys.refresh_all();

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

    let disks = Disks::new_with_refreshed_list();
    let mut disk_usage = 0.0;
    for disk in disks.iter() {
        let total_space = disk.total_space();
        let available_space = disk.available_space();
        let used_space = total_space.saturating_sub(available_space);
        if total_space > 0 {
            disk_usage = (used_space as f64 / total_space as f64) * 100.0;
            break;
        }
    }

    let networks = Networks::new_with_refreshed_list();
    let mut network_rx = 0;
    let mut network_tx = 0;
    for network in networks.values() {
        network_rx += network.received();
        network_tx += network.transmitted();
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
