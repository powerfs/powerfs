//! Minimal RDMA pingpong test using powerfs-net Transport + TransportStream split.
//!
//! Usage (host net namespace, needs rxe0 + powerfs-br0 172.30.0.1):
//!   Server:  RUST_LOG=debug cargo run --features rdma --example rdma_pingpong -- --server 0.0.0.0:19334 --device rxe0
//!   Client:  RUST_LOG=debug cargo run --features rdma --example rdma_pingpong -- --client 172.30.0.1:19334 --device rxe0
//!
//! Performs 10 ping-pong rounds then exits.

use std::io::Write;
use std::net::SocketAddr;
use std::time::Instant;

use clap::Parser;
use powerfs_net::{create_transport, Transport, TransportConfig, TransportStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    server: Option<SocketAddr>,
    #[arg(long)]
    client: Option<SocketAddr>,
    #[arg(long, default_value = "rdma")]
    transport: String,
    #[arg(long)]
    device: Option<String>,
    #[arg(long, default_value_t = 10)]
    iters: usize,
    #[arg(long, default_value_t = 64)]
    payload: usize,
}

fn make_cfg(args: &Args) -> TransportConfig {
    let mut cfg = TransportConfig::default();
    cfg.transport = args.transport.clone();
    cfg.rdma_device = args.device.clone();
    cfg.rdma_buf_num = 4;
    cfg.rdma_buf_size = 65536;
    cfg.tcp_fallback = false;
    cfg
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_default_env()
        .format(|buf, r| {
            writeln!(
                buf,
                "[{:5} {}] {}",
                r.level(),
                r.module_path().unwrap_or("?"),
                r.args()
            )
        })
        .init();

    let args = Args::parse();
    let cfg = make_cfg(&args);
    let transport = create_transport(&cfg)?;
    println!(
        "Transport: {} rdma_device={:?}",
        transport.name(),
        cfg.rdma_device
    );

    let iters = args.iters;
    let n = args.payload;

    if let Some(bind) = args.server {
        let listener = transport.bind(bind).await?;
        println!("SERVER listening on {}", bind);
        let stream = listener.accept().await?;
        println!("SERVER accepted conn, peer={}", stream.peer_addr());
        server_loop(stream, iters, n).await?;
    } else if let Some(remote) = args.client {
        println!("CLIENT connecting to {}...", remote);
        let start = Instant::now();
        let stream = transport.connect(remote).await?;
        println!("CLIENT connected in {:?}", start.elapsed());
        client_loop(stream, iters, n).await?;
    } else {
        anyhow::bail!("must pass --server or --client");
    }
    Ok(())
}

async fn server_loop(stream: Box<dyn TransportStream>, iters: usize, n: usize) -> anyhow::Result<()> {
    let (mut r, mut w) = stream.split();
    let mut buf = vec![0u8; n];
    let t0 = Instant::now();
    for i in 0..iters {
        r.read_exact(&mut buf).await?;
        // flip all bytes
        for b in buf.iter_mut() {
            *b ^= 0xff;
        }
        w.write_all(&buf).await?;
        w.flush().await?;
        if (i + 1) % 1 == 0 {
            eprintln!("[srv] round {}/{} OK", i + 1, iters);
        }
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "[srv] DONE. {} iters, payload={} bytes, total={:.3} ms",
        iters,
        n,
        elapsed.as_secs_f64() * 1000.0
    );
    Ok(())
}

async fn client_loop(stream: Box<dyn TransportStream>, iters: usize, n: usize) -> anyhow::Result<()> {
    let (mut r, mut w) = stream.split();
    let mut send = vec![0xABu8; n];
    let mut recv = vec![0u8; n];
    let t0 = Instant::now();
    for i in 0..iters {
        w.write_all(&send).await?;
        w.flush().await?;
        r.read_exact(&mut recv).await?;
        // server flips bits: expected ^= 0xFF
        let mut expected = send.clone();
        for b in expected.iter_mut() {
            *b ^= 0xff;
        }
        assert_eq!(expected, recv, "round {} payload mismatch", i);
        eprintln!("[cli] round {}/{} OK ({}B RTT)", i + 1, iters, n);
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "[cli] DONE. {} iters, payload={} bytes, total={:.3} ms, avg={:.3} us/iter",
        iters,
        n,
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / (iters as f64)
    );
    Ok(())
}
