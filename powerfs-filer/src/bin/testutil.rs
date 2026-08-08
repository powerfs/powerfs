//! powerfs-testutil — EC 降级读测试辅助工具.
//!
//! 通过 powerfs-net TLV 协议直接操作 Volume Server 上的 needle,
//! 供集成测试脚本模拟分片丢失 (删除指定 needle 使对应 shard 不可读).
//!
//! 用法:
//!   powerfs-testutil delete-needle --addr 172.30.0.21:8901 --volume-id 5 --needle-id 0x1234
//!
//!   powerfs-testutil read-needle  --addr 172.30.0.21:8901 --volume-id 5 --needle-id 0x1234
//!     (读取 needle 数据并输出到 stdout, 用于校验 shard 内容 / 确认删除生效)

use clap::{Parser, Subcommand};
use powerfs_filer::TlvVolumeClient;
use powerfs_net::{ClientConnPool, ClientPoolConfig};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "powerfs-testutil")]
#[command(about = "PowerFS EC 降级读测试辅助工具 (TLV needle 操作)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 删除指定 volume 上的 needle (模拟分片丢失)
    DeleteNeedle {
        /// Volume Server 的 powerfs-net 地址 (ip:port)
        #[arg(long)]
        addr: String,
        /// volume_id
        #[arg(long)]
        volume_id: u64,
        /// needle_id (file_key), 支持 0x 十六进制
        #[arg(long)]
        needle_id: String,
    },
    /// 读取指定 volume 上的 needle 数据并输出到 stdout
    ReadNeedle {
        /// Volume Server 的 powerfs-net 地址 (ip:port)
        #[arg(long)]
        addr: String,
        /// volume_id
        #[arg(long)]
        volume_id: u64,
        /// needle_id (file_key), 支持 0x 十六进制
        #[arg(long)]
        needle_id: String,
    },
}

fn parse_u64(s: &str) -> Result<u64, String> {
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(rest, 16)
    } else {
        s.parse::<u64>()
    }
    .map_err(|e| format!("invalid number '{}': {}", s, e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Warn)
        .init();

    let cli = Cli::parse();

    // 构造 TLV Volume 客户端 (与 Filer scrubber 共用同一协议)
    let pool = Arc::new(ClientConnPool::new(
        0, // client_id (测试工具无需正式注册)
        ClientPoolConfig::default(),
        None,
    ));
    let client = TlvVolumeClient::new(pool);

    match cli.command {
        Command::DeleteNeedle {
            addr,
            volume_id,
            needle_id,
        } => {
            let nid = parse_u64(&needle_id)?;
            print!(
                "deleting needle: addr={} volume_id={} needle_id={:#x} ... ",
                addr, volume_id, nid
            );
            std::io::Write::flush(&mut std::io::stdout())?;
            match client.delete_needle(&addr, volume_id, nid).await {
                Ok(()) => {
                    println!("OK");
                    Ok(())
                }
                Err(e) => {
                    println!("FAILED");
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Command::ReadNeedle {
            addr,
            volume_id,
            needle_id,
        } => {
            let nid = parse_u64(&needle_id)?;
            match client.read_needle(&addr, volume_id, nid).await {
                Ok(data) => {
                    // 输出字节数到 stderr, 数据到 stdout
                    eprintln!("read {} bytes", data.len());
                    std::io::Write::write_all(&mut std::io::stdout(), &data)?;
                    Ok(())
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}
