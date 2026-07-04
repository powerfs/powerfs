use clap::{Parser, Subcommand};

use crate::kv_client::KvCacheClient;

#[derive(Parser)]
pub struct KvArgs {
    #[command(subcommand)]
    command: KvCommands,
}

#[derive(Subcommand)]
pub enum KvCommands {
    Session(KvSessionArgs),
    Block(KvBlockArgs),
    List(KvListArgs),
    Stats(KvStatsArgs),
}

#[derive(Parser)]
pub struct KvSessionArgs {
    #[command(subcommand)]
    command: SessionCommands,
}

#[derive(Subcommand)]
pub enum SessionCommands {
    Create {
        #[arg(long, short)]
        session_id: String,

        #[arg(long, short)]
        model_name: String,

        #[arg(long, default_value = "32")]
        num_layers: u32,

        #[arg(long, default_value = "32")]
        num_heads: u32,

        #[arg(long, default_value = "128")]
        head_dim: u32,

        #[arg(long, default_value = "fp16")]
        dtype: String,

        #[arg(long, default_value = "3600")]
        ttl_seconds: u64,
    },

    Delete {
        #[arg(long, short)]
        session_id: String,
    },

    Get {
        #[arg(long, short)]
        session_id: String,
    },
}

#[derive(Parser)]
pub struct KvBlockArgs {
    #[command(subcommand)]
    command: BlockCommands,
}

#[derive(Subcommand)]
pub enum BlockCommands {
    Put {
        #[arg(long, short)]
        session_id: String,

        #[arg(long, short)]
        layer_id: u32,

        #[arg(long, short)]
        num_tokens: u32,

        #[arg(long, short)]
        data: String,

        #[arg(long, short, default_value = "bytes")]
        format: String,
    },

    Get {
        #[arg(long, short)]
        block_id: u64,
    },

    Delete {},
}

#[derive(Parser)]
pub struct KvListArgs {
    #[arg(long, default_value = "100")]
    limit: u32,

    #[arg(long, default_value = "")]
    prefix: String,
}

#[derive(Parser)]
pub struct KvStatsArgs {}

pub async fn kv(client: KvCacheClient, args: KvArgs) -> super::CommandResult {
    match args.command {
        KvCommands::Session(session_args) => kv_session(client, session_args).await,
        KvCommands::Block(block_args) => kv_block(client, block_args).await,
        KvCommands::List(list_args) => kv_list(client, list_args).await,
        KvCommands::Stats(stats_args) => kv_stats(client, stats_args).await,
    }
}

async fn kv_session(mut client: KvCacheClient, args: KvSessionArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    match args.command {
        SessionCommands::Create {
            session_id,
            model_name,
            num_layers,
            num_heads,
            head_dim,
            dtype,
            ttl_seconds,
        } => {
            let req = crate::kv_client::CreateSessionRequest {
                session_id,
                model_name,
                num_layers,
                num_heads,
                head_dim,
                dtype,
                ttl_seconds,
            };

            let resp = svc.create_session(req).await.map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
            })?;
            let resp = resp.into_inner();

            if resp.success {
                println!("Session created successfully");
            } else {
                eprintln!("Failed to create session: {}", resp.error);
                std::process::exit(1);
            }
        }

        SessionCommands::Delete { session_id } => {
            let req = crate::kv_client::DeleteSessionRequest { session_id };
            let resp = svc.delete_session(req).await.map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
            })?;
            let resp = resp.into_inner();

            if resp.success {
                println!("Session deleted successfully");
            } else {
                eprintln!("Failed to delete session: {}", resp.error);
                std::process::exit(1);
            }
        }

        SessionCommands::Get { session_id } => {
            let req = crate::kv_client::GetSessionRequest { session_id };
            let resp = svc.get_session(req).await.map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
            })?;
            let resp = resp.into_inner();

            if resp.exists {
                println!(
                    "Session: {} (Model: {}, Layers: {}, Blocks: {}, Tokens: {}, Used: {} bytes)",
                    resp.session_id,
                    resp.model_name,
                    resp.num_layers,
                    resp.num_blocks,
                    resp.total_tokens,
                    resp.used_bytes
                );
            } else {
                eprintln!("Session not found");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

async fn kv_block(mut client: KvCacheClient, args: KvBlockArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    match args.command {
        BlockCommands::Put {
            session_id,
            layer_id,
            num_tokens,
            data,
            format,
        } => {
            let data_bytes = match format.as_str() {
                "bytes" => data.as_bytes().to_vec(),
                "hex" => decode_hex(&data),
                "base64" => decode_base64(&data),
                _ => {
                    eprintln!("Unknown format: {}", format);
                    std::process::exit(1);
                }
            };

            let req = crate::kv_client::PutBlockRequest {
                session_id,
                layer_id,
                num_tokens,
                data: data_bytes,
            };

            let resp = svc.put_block(req).await.map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
            })?;
            let resp = resp.into_inner();

            if resp.success {
                println!("Block put successfully, block_id: {}", resp.block_id);
            } else {
                eprintln!("Failed to put block: {}", resp.error);
                std::process::exit(1);
            }
        }

        BlockCommands::Get { block_id } => {
            let req = crate::kv_client::GetBlockRequest { block_id };
            let resp = svc.get_block(req).await.map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
            })?;
            let resp = resp.into_inner();

            if resp.found {
                println!(
                    "Block: {} (Layer: {}, Tokens: {}, Data size: {} bytes)",
                    resp.block_id,
                    resp.layer_id,
                    resp.num_tokens,
                    resp.data.len()
                );
                println!("Data (hex): {}", encode_hex(&resp.data));
            } else {
                eprintln!("Block not found: {}", resp.error);
                std::process::exit(1);
            }
        }

        BlockCommands::Delete {} => {
            eprintln!("Block delete not supported via KV Cache API, use session delete");
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn kv_list(mut client: KvCacheClient, args: KvListArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let req = crate::kv_client::ListSessionsRequest {
        limit: args.limit,
        prefix: args.prefix,
    };

    let resp = svc
        .list_sessions(req)
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e)))?;
    let resp = resp.into_inner();

    println!("Total sessions: {}", resp.total);
    println!("Session IDs:");
    for id in resp.session_ids {
        println!("  {}", id);
    }

    Ok(())
}

async fn kv_stats(mut client: KvCacheClient, _args: KvStatsArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let req = crate::kv_client::GetStatsRequest {};
    let resp = svc
        .get_stats(req)
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e)))?;
    let resp = resp.into_inner();

    let used_gb = resp.used_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let max_gb = resp.max_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("KV Cache Statistics:");
    println!("  Total sessions: {}", resp.total_sessions);
    println!("  Total blocks: {}", resp.total_blocks);
    println!("  Used memory: {:.2} GB / {:.2} GB", used_gb, max_gb);
    println!("  Cache hits: {}", resp.cache_hits);
    println!("  Cache misses: {}", resp.cache_misses);
    println!("  Evictions: {}", resp.evictions);

    let hit_ratio = if resp.cache_hits + resp.cache_misses > 0 {
        (resp.cache_hits as f64 / (resp.cache_hits + resp.cache_misses) as f64) * 100.0
    } else {
        0.0
    };
    println!("  Hit ratio: {:.2}%", hit_ratio);

    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn decode_hex(s: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(s.len() / 2);
    let chars: Vec<char> = s.chars().collect();
    for i in (0..chars.len()).step_by(2) {
        if let Some(c1) = chars.get(i) {
            if let Some(c2) = chars.get(i + 1) {
                let hex = format!("{}{}", c1, c2);
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    bytes.push(byte);
                } else {
                    eprintln!("Invalid hex: {}", hex);
                    std::process::exit(1);
                }
            } else {
                eprintln!("Invalid hex length");
                std::process::exit(1);
            }
        }
    }
    bytes
}

fn decode_base64(s: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    let chars: Vec<char> = s.chars().filter(|c| !c.is_whitespace()).collect();
    let table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut i = 0;
    while i < chars.len() {
        let mut bits = 0u32;
        let mut valid = 0;

        for _ in 0..4 {
            if i >= chars.len() {
                break;
            }
            let c = chars[i];
            i += 1;
            if c == '=' {
                continue;
            }
            if let Some(pos) = table.find(c) {
                bits = (bits << 6) | pos as u32;
                valid += 1;
            } else {
                eprintln!("Invalid base64 character: {}", c);
                std::process::exit(1);
            }
        }

        if valid >= 2 {
            bytes.push(((bits >> 16) & 0xFF) as u8);
        }
        if valid >= 3 {
            bytes.push(((bits >> 8) & 0xFF) as u8);
        }
        if valid >= 4 {
            bytes.push((bits & 0xFF) as u8);
        }
    }

    bytes
}
