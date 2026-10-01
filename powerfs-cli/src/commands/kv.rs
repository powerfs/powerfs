use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::kv_client::KvCacheClient;

// ============================================================
// Command tree
// ============================================================

#[derive(Parser)]
pub struct KvArgs {
    #[command(subcommand)]
    command: KvCommands,
}

#[derive(Subcommand)]
pub enum KvCommands {
    /// LLM inference session management
    Session(KvSessionArgs),
    /// LLM KV-cache block read/write
    Block(KvBlockArgs),
    /// Namespace management
    Namespace(KvNamespaceArgs),
    /// Put a key/value pair
    Put(KvPutArgs),
    /// Get a value by key
    Get(KvGetArgs),
    /// Delete a key
    Delete(KvDeleteArgs),
    /// Check whether a key exists
    Exists(KvExistsArgs),
    /// List keys in a namespace
    List(KvListArgs),
    /// Remove keys matching a regular expression
    RemoveRegex(KvRemoveRegexArgs),
    /// Remove all keys in a namespace
    RemoveAll(KvRemoveAllArgs),
    /// Put multiple key/value pairs in one request
    BatchPut(KvBatchPutArgs),
    /// Get multiple keys in one request
    BatchGet(KvBatchGetArgs),
    /// Show KV cache statistics
    Stats(KvStatsArgs),
}

// ---------------- Session ----------------

#[derive(Parser)]
pub struct KvSessionArgs {
    #[command(subcommand)]
    command: SessionCommands,
}

#[derive(Subcommand)]
enum SessionCommands {
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

        #[arg(long)]
        owner_id: Option<String>,

        #[arg(long)]
        namespace_id: Option<String>,
    },

    Delete {
        #[arg(long, short)]
        session_id: String,
    },

    Get {
        #[arg(long, short)]
        session_id: String,
    },

    List {
        #[arg(long, default_value = "100")]
        limit: u32,

        #[arg(long, default_value = "")]
        prefix: String,
    },
}

// ---------------- Block ----------------

#[derive(Parser)]
pub struct KvBlockArgs {
    #[command(subcommand)]
    command: BlockCommands,
}

#[derive(Subcommand)]
enum BlockCommands {
    Put {
        #[arg(long, short)]
        session_id: String,

        #[arg(long, short)]
        layer_id: u32,

        #[arg(long, short)]
        num_tokens: u32,

        /// Inline value (interpreted according to --format)
        #[arg(long, short)]
        data: Option<String>,

        /// Read raw bytes from a file
        #[arg(long, short)]
        file: Option<PathBuf>,

        /// Read raw bytes from stdin
        #[arg(long)]
        stdin: bool,

        #[arg(long, short, default_value = "bytes")]
        format: String,
    },

    Get {
        #[arg(long, short)]
        block_id: u64,

        /// Write raw bytes to this file instead of stdout
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Output encoding when writing to stdout
        #[arg(long, default_value = "hex")]
        format: String,
    },

    BatchPut {
        #[arg(long, short)]
        session_id: String,

        /// Block spec, repeatable: "<layer-id>:<num-tokens>:<file-with-raw-bytes>"
        #[arg(long = "block", short = 'B')]
        blocks: Vec<String>,
    },

    BatchGet {
        #[arg(long, short)]
        block_id: Vec<u64>,
    },
}

// ---------------- Namespace ----------------

#[derive(Parser)]
pub struct KvNamespaceArgs {
    #[command(subcommand)]
    command: NamespaceCommands,
}

#[derive(Subcommand)]
enum NamespaceCommands {
    Create {
        #[arg(long, short)]
        namespace_id: String,

        #[arg(long)]
        name: String,

        #[arg(long)]
        owner_id: Option<String>,
    },

    List {
        /// Filter by owner; omit to list every namespace
        #[arg(long)]
        owner_id: Option<String>,
    },

    Get {
        #[arg(long, short)]
        namespace_id: String,
    },

    Delete {
        #[arg(long, short)]
        namespace_id: String,

        #[arg(long)]
        owner_id: Option<String>,

        /// Skip interactive confirmation
        #[arg(long)]
        yes: bool,
    },
}

// ---------------- Generic KV ----------------

#[derive(Parser)]
pub struct KvPutArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    key: String,

    /// Inline value (interpreted according to --format)
    #[arg(long, short)]
    data: Option<String>,

    /// Read raw bytes from a file
    #[arg(long, short)]
    file: Option<PathBuf>,

    /// Read raw bytes from stdin
    #[arg(long)]
    stdin: bool,

    #[arg(long, short, default_value = "bytes")]
    format: String,

    #[arg(long)]
    owner_id: Option<String>,
}

#[derive(Parser)]
pub struct KvGetArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    key: String,

    /// Write raw bytes to this file instead of stdout
    #[arg(long, short)]
    output: Option<PathBuf>,

    /// Output encoding when writing to stdout
    #[arg(long, default_value = "bytes")]
    format: String,
}

#[derive(Parser)]
pub struct KvDeleteArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    key: String,
}

#[derive(Parser)]
pub struct KvExistsArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    key: String,
}

#[derive(Parser)]
pub struct KvListArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, default_value = "")]
    prefix: String,
}

#[derive(Parser)]
pub struct KvRemoveRegexArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    pattern: String,

    /// Skip interactive confirmation
    #[arg(long)]
    yes: bool,
}

#[derive(Parser)]
pub struct KvRemoveAllArgs {
    #[arg(long, short)]
    namespace_id: String,

    /// Skip interactive confirmation
    #[arg(long)]
    yes: bool,
}

#[derive(Parser)]
pub struct KvBatchPutArgs {
    #[arg(long, short)]
    namespace_id: String,

    /// Pair, repeatable: "<key>=<value>". A value starting with '@' is read
    /// raw from the given file path; otherwise it is decoded per --format.
    #[arg(long, short)]
    pair: Vec<String>,

    #[arg(long, short, default_value = "bytes")]
    format: String,

    #[arg(long)]
    owner_id: Option<String>,
}

#[derive(Parser)]
pub struct KvBatchGetArgs {
    #[arg(long, short)]
    namespace_id: String,

    #[arg(long, short)]
    key: Vec<String>,

    /// Output encoding for returned values
    #[arg(long, default_value = "hex")]
    format: String,
}

#[derive(Parser)]
pub struct KvStatsArgs {}

// ============================================================
// Dispatch
// ============================================================

pub async fn kv(client: KvCacheClient, args: KvArgs) -> super::CommandResult {
    match args.command {
        KvCommands::Session(session_args) => kv_session(client, session_args).await,
        KvCommands::Block(block_args) => kv_block(client, block_args).await,
        KvCommands::Namespace(ns_args) => kv_namespace(client, ns_args).await,
        KvCommands::Put(put_args) => kv_put(client, put_args).await,
        KvCommands::Get(get_args) => kv_get(client, get_args).await,
        KvCommands::Delete(del_args) => kv_delete(client, del_args).await,
        KvCommands::Exists(exists_args) => kv_exists(client, exists_args).await,
        KvCommands::List(list_args) => kv_list(client, list_args).await,
        KvCommands::RemoveRegex(args) => kv_remove_regex(client, args).await,
        KvCommands::RemoveAll(args) => kv_remove_all(client, args).await,
        KvCommands::BatchPut(args) => kv_batch_put(client, args).await,
        KvCommands::BatchGet(args) => kv_batch_get(client, args).await,
        KvCommands::Stats(stats_args) => kv_stats(client, stats_args).await,
    }
}

// ---------------- Session handlers ----------------

async fn kv_session(mut client: KvCacheClient, args: KvSessionArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    match args.command {
        SessionCommands::Create {
            session_id,
            model_name,
            num_layers,
            num_heads,
            head_dim,
            dtype,
            ttl_seconds,
            owner_id,
            namespace_id,
        } => {
            let req = crate::kv_client::CreateSessionRequest {
                session_id,
                model_name,
                num_layers,
                num_heads,
                head_dim,
                dtype,
                ttl_seconds,
                owner_id: owner_id.unwrap_or_default(),
                namespace_id: namespace_id.unwrap_or_default(),
                collection: String::new(),
            };

            let resp = svc.create_session(req).await.map_err(rpc_err)?.into_inner();

            if resp.success {
                println!("Session created successfully");
            } else {
                eprintln!("Failed to create session: {}", resp.error);
                std::process::exit(1);
            }
        }

        SessionCommands::Delete { session_id } => {
            let req = crate::kv_client::DeleteSessionRequest { session_id };
            let resp = svc.delete_session(req).await.map_err(rpc_err)?.into_inner();

            if resp.success {
                println!("Session deleted successfully");
            } else {
                eprintln!("Failed to delete session: {}", resp.error);
                std::process::exit(1);
            }
        }

        SessionCommands::Get { session_id } => {
            let req = crate::kv_client::GetSessionRequest { session_id };
            let resp = svc.get_session(req).await.map_err(rpc_err)?.into_inner();

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

        SessionCommands::List { limit, prefix } => {
            let req = crate::kv_client::ListSessionsRequest { limit, prefix };
            let resp = svc.list_sessions(req).await.map_err(rpc_err)?.into_inner();

            println!("Total sessions: {}", resp.total);
            for id in resp.session_ids {
                println!("{}", id);
            }
        }
    }

    Ok(())
}

// ---------------- Block handlers ----------------

async fn kv_block(mut client: KvCacheClient, args: KvBlockArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    match args.command {
        BlockCommands::Put {
            session_id,
            layer_id,
            num_tokens,
            data,
            file,
            stdin,
            format,
        } => {
            let data_bytes = read_value(data, file, stdin, &format)?;

            let req = crate::kv_client::PutBlockRequest {
                session_id,
                layer_id,
                num_tokens,
                data: data_bytes,
            };

            let resp = svc.put_block(req).await.map_err(rpc_err)?.into_inner();

            if resp.success {
                println!("Block put successfully, block_id: {}", resp.block_id);
            } else {
                eprintln!("Failed to put block: {}", resp.error);
                std::process::exit(1);
            }
        }

        BlockCommands::Get {
            block_id,
            output,
            format,
        } => {
            let req = crate::kv_client::GetBlockRequest { block_id };
            let resp = svc.get_block(req).await.map_err(rpc_err)?.into_inner();

            if resp.found {
                println!(
                    "Block: {} (Layer: {}, Tokens: {}, Data size: {} bytes)",
                    resp.block_id,
                    resp.layer_id,
                    resp.num_tokens,
                    resp.data.len()
                );
                write_value(&resp.data, &format, output)?;
            } else {
                eprintln!("Block not found: {}", resp.error);
                std::process::exit(1);
            }
        }

        BlockCommands::BatchPut { session_id, blocks } => {
            if blocks.is_empty() {
                eprintln!("At least one --block spec is required");
                std::process::exit(1);
            }

            let mut requests = Vec::with_capacity(blocks.len());
            for spec in &blocks {
                let parts: Vec<&str> = spec.splitn(3, ':').collect();
                if parts.len() != 3 {
                    eprintln!(
                        "Invalid block spec '{}': expected '<layer-id>:<num-tokens>:<file>'",
                        spec
                    );
                    std::process::exit(1);
                }
                let layer_id = parse_or_exit(parts[0], "layer-id");
                let num_tokens = parse_or_exit(parts[1], "num-tokens");
                let data = std::fs::read(parts[2]).unwrap_or_else(|e| {
                    eprintln!("Cannot read block file '{}': {}", parts[2], e);
                    std::process::exit(1);
                });

                requests.push(crate::kv_client::PutBlockRequest {
                    session_id: session_id.clone(),
                    layer_id,
                    num_tokens,
                    data,
                });
            }

            let req = crate::kv_client::BatchPutRequest { blocks: requests };
            let resp = svc.batch_put(req).await.map_err(rpc_err)?.into_inner();

            for (i, r) in resp.results.iter().enumerate() {
                if r.success {
                    println!("[{}] block_id: {}", i, r.block_id);
                } else {
                    println!("[{}] failed: {}", i, r.error);
                }
            }
        }

        BlockCommands::BatchGet { block_id } => {
            if block_id.is_empty() {
                eprintln!("At least one --block-id is required");
                std::process::exit(1);
            }

            let req = crate::kv_client::BatchGetRequest {
                block_ids: block_id,
            };
            let resp = svc.batch_get(req).await.map_err(rpc_err)?.into_inner();

            for b in resp.blocks {
                if b.found {
                    println!(
                        "Block: {} (Layer: {}, Tokens: {}, Data size: {} bytes)",
                        b.block_id,
                        b.layer_id,
                        b.num_tokens,
                        b.data.len()
                    );
                    println!("Data (hex): {}", encode_hex(&b.data));
                    println!();
                } else {
                    println!("Block {} not found: {}", b.block_id, b.error);
                }
            }
        }
    }

    Ok(())
}

// ---------------- Namespace handlers ----------------

async fn kv_namespace(mut client: KvCacheClient, args: KvNamespaceArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    match args.command {
        NamespaceCommands::Create {
            namespace_id,
            name,
            owner_id,
        } => {
            let req = crate::kv_client::CreateNamespaceRequest {
                namespace_id,
                name,
                owner_id: owner_id.unwrap_or_default(),
            };
            let resp = svc
                .create_namespace(req)
                .await
                .map_err(rpc_err)?
                .into_inner();

            if resp.success {
                println!("Namespace created: {}", resp.namespace_id);
            } else {
                eprintln!("Failed to create namespace: {}", resp.error);
                std::process::exit(1);
            }
        }

        NamespaceCommands::List { owner_id } => {
            let req = crate::kv_client::ListNamespacesRequest {
                owner_id: owner_id.unwrap_or_default(),
            };
            let resp = svc
                .list_namespaces(req)
                .await
                .map_err(rpc_err)?
                .into_inner();

            if !resp.error.is_empty() {
                eprintln!("Failed to list namespaces: {}", resp.error);
                std::process::exit(1);
            }

            println!(
                "{:<24} {:<20} {:<20} {:<20} {:<20}",
                "ID", "NAME", "OWNER", "CREATED", "UPDATED"
            );
            for ns in resp.namespaces {
                println!(
                    "{:<24} {:<20} {:<20} {:<20} {:<20}",
                    ns.id, ns.name, ns.owner_id, ns.created_at, ns.updated_at
                );
            }
        }

        NamespaceCommands::Get { namespace_id } => {
            let req = crate::kv_client::GetNamespaceRequest {
                namespace_id,
                owner_id: String::new(),
            };
            let resp = svc.get_namespace(req).await.map_err(rpc_err)?.into_inner();

            if resp.found {
                let ns = resp
                    .namespace
                    .unwrap_or_else(|| crate::kv_client::KvNamespace {
                        id: String::new(),
                        name: String::new(),
                        owner_id: String::new(),
                        created_at: 0,
                        updated_at: 0,
                    });
                println!("ID:        {}", ns.id);
                println!("Name:      {}", ns.name);
                println!("Owner:     {}", ns.owner_id);
                println!("Created:   {}", ns.created_at);
                println!("Updated:   {}", ns.updated_at);
            } else {
                eprintln!("Namespace not found: {}", resp.error);
                std::process::exit(1);
            }
        }

        NamespaceCommands::Delete {
            namespace_id,
            owner_id,
            yes,
        } => {
            if !confirm_destructive("delete namespace", &namespace_id, yes) {
                std::process::exit(1);
            }

            let req = crate::kv_client::DeleteNamespaceRequest {
                namespace_id,
                owner_id: owner_id.unwrap_or_default(),
            };
            let resp = svc
                .delete_namespace(req)
                .await
                .map_err(rpc_err)?
                .into_inner();

            if resp.success {
                println!("Namespace deleted successfully");
            } else {
                eprintln!("Failed to delete namespace: {}", resp.error);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

// ---------------- Generic KV handlers ----------------

async fn kv_put(mut client: KvCacheClient, args: KvPutArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let value = read_value(args.data, args.file, args.stdin, &args.format)?;

    let req = crate::kv_client::KvPutRequest {
        namespace_id: args.namespace_id,
        key: args.key,
        value,
        owner_id: args.owner_id.unwrap_or_default(),
        ttl_seconds: 0,
    };
    let resp = svc.kv_put(req).await.map_err(rpc_err)?.into_inner();

    if resp.success {
        println!("OK");
    } else {
        eprintln!("Failed to put key: {}", resp.error);
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_get(mut client: KvCacheClient, args: KvGetArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let req = crate::kv_client::KvGetRequest {
        namespace_id: args.namespace_id,
        key: args.key,
    };
    let resp = svc.kv_get(req).await.map_err(rpc_err)?.into_inner();

    if !resp.success {
        eprintln!("Failed to get key: {}", resp.error);
        std::process::exit(1);
    }

    if resp.found {
        write_value(&resp.value, &args.format, args.output)?;
    } else {
        eprintln!("Key not found");
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_delete(mut client: KvCacheClient, args: KvDeleteArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let req = crate::kv_client::KvDeleteRequest {
        namespace_id: args.namespace_id,
        key: args.key,
    };
    let resp = svc.kv_delete(req).await.map_err(rpc_err)?.into_inner();

    if resp.success {
        println!("OK");
    } else {
        eprintln!("Failed to delete key: {}", resp.error);
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_exists(mut client: KvCacheClient, args: KvExistsArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let req = crate::kv_client::KvExistsRequest {
        namespace_id: args.namespace_id,
        key: args.key,
    };
    let resp = svc.kv_exists(req).await.map_err(rpc_err)?.into_inner();

    if !resp.error.is_empty() {
        eprintln!("Failed to check key: {}", resp.error);
        std::process::exit(2);
    }

    if resp.exists {
        println!("exists");
    } else {
        println!("not found");
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_list(mut client: KvCacheClient, args: KvListArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let req = crate::kv_client::KvListRequest {
        namespace_id: args.namespace_id,
        prefix: args.prefix,
    };
    let resp = svc.kv_list(req).await.map_err(rpc_err)?.into_inner();

    if !resp.error.is_empty() {
        eprintln!("Failed to list keys: {}", resp.error);
        std::process::exit(1);
    }

    for key in resp.keys {
        println!("{}", key);
    }

    Ok(())
}

async fn kv_remove_regex(
    mut client: KvCacheClient,
    args: KvRemoveRegexArgs,
) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let target = format!(
        "keys matching '{}' in namespace '{}'",
        args.pattern, args.namespace_id
    );
    if !confirm_destructive("remove", &target, args.yes) {
        std::process::exit(1);
    }

    let req = crate::kv_client::KvRemoveByRegexRequest {
        namespace_id: args.namespace_id,
        pattern: args.pattern,
    };
    let resp = svc
        .kv_remove_by_regex(req)
        .await
        .map_err(rpc_err)?
        .into_inner();

    if resp.success {
        println!("OK");
    } else {
        eprintln!("Failed to remove keys: {}", resp.error);
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_remove_all(mut client: KvCacheClient, args: KvRemoveAllArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let target = format!("all keys in namespace '{}'", args.namespace_id);
    if !confirm_destructive("remove", &target, args.yes) {
        std::process::exit(1);
    }

    let req = crate::kv_client::KvRemoveAllRequest {
        namespace_id: args.namespace_id,
    };
    let resp = svc.kv_remove_all(req).await.map_err(rpc_err)?.into_inner();

    if resp.success {
        println!("OK");
    } else {
        eprintln!("Failed to remove keys: {}", resp.error);
        std::process::exit(1);
    }

    Ok(())
}

async fn kv_batch_put(mut client: KvCacheClient, args: KvBatchPutArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    if args.pair.is_empty() {
        eprintln!("At least one --pair is required");
        std::process::exit(1);
    }

    let mut keys = Vec::with_capacity(args.pair.len());
    let mut values = Vec::with_capacity(args.pair.len());

    for pair in &args.pair {
        let (key, spec) = match pair.splitn(2, '=').collect::<Vec<_>>()[..] {
            [k, v] => (k.to_string(), v.to_string()),
            _ => {
                eprintln!("Invalid pair '{}': expected '<key>=<value>'", pair);
                std::process::exit(1);
            }
        };

        let value = if let Some(path) = spec.strip_prefix('@') {
            std::fs::read(path).unwrap_or_else(|e| {
                eprintln!("Cannot read value file '{}': {}", path, e);
                std::process::exit(1);
            })
        } else {
            decode_inline(&spec, &args.format)
        };

        keys.push(key);
        values.push(value);
    }

    let req = crate::kv_client::KvBatchPutRequest {
        namespace_id: args.namespace_id,
        keys,
        values,
        owner_id: args.owner_id.unwrap_or_default(),
    };
    let resp = svc.kv_batch_put(req).await.map_err(rpc_err)?.into_inner();

    if !resp.error.is_empty() {
        eprintln!("Failed batch put: {}", resp.error);
        std::process::exit(1);
    }

    for (i, ok) in resp.successes.iter().enumerate() {
        println!("[{}] {}", i, if *ok { "OK" } else { "FAILED" });
    }

    Ok(())
}

async fn kv_batch_get(mut client: KvCacheClient, args: KvBatchGetArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    if args.key.is_empty() {
        eprintln!("At least one --key is required");
        std::process::exit(1);
    }

    let keys = args.key.clone();
    let req = crate::kv_client::KvBatchGetRequest {
        namespace_id: args.namespace_id,
        keys: args.key,
    };
    let resp = svc.kv_batch_get(req).await.map_err(rpc_err)?.into_inner();

    if !resp.error.is_empty() {
        eprintln!("Failed batch get: {}", resp.error);
        std::process::exit(1);
    }

    for (i, key) in keys.iter().enumerate() {
        let found = resp.found.get(i).copied().unwrap_or(false);
        if found {
            let value = resp.values.get(i).cloned().unwrap_or_default();
            println!(
                "{} ({} bytes): {}",
                key,
                value.len(),
                encode_format(&value, &args.format)
            );
        } else {
            println!("{}: not found", key);
        }
    }

    Ok(())
}

async fn kv_stats(mut client: KvCacheClient, _args: KvStatsArgs) -> super::CommandResult {
    let mut svc = client.service().await.map_err(connect_err)?;

    let req = crate::kv_client::GetStatsRequest {};
    let resp = svc.get_stats(req).await.map_err(rpc_err)?.into_inner();

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

// ============================================================
// Helpers
// ============================================================

fn connect_err(e: Box<dyn std::error::Error>) -> powerfs_common::error::PowerFsError {
    powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
}

fn rpc_err(e: tonic::Status) -> powerfs_common::error::PowerFsError {
    powerfs_common::error::PowerFsError::Internal(format!("RPC error: {}", e))
}

fn parse_or_exit<T: std::str::FromStr>(s: &str, field: &str) -> T {
    s.parse().unwrap_or_else(|_| {
        eprintln!("Invalid {}: '{}'", field, s);
        std::process::exit(1);
    })
}

/// Read a value from exactly one of: inline data / file / stdin.
fn read_value(
    data: Option<String>,
    file: Option<PathBuf>,
    from_stdin: bool,
    format: &str,
) -> Result<Vec<u8>, powerfs_common::error::PowerFsError> {
    let sources = u8::from(data.is_some()) + u8::from(file.is_some()) + u8::from(from_stdin);
    if sources != 1 {
        return Err(powerfs_common::error::PowerFsError::Internal(
            "Specify exactly one value source: --data, --file or --stdin".to_string(),
        ));
    }

    if let Some(path) = file {
        std::fs::read(&path).map_err(|e| {
            powerfs_common::error::PowerFsError::Internal(format!(
                "Cannot read file '{}': {}",
                path.display(),
                e
            ))
        })
    } else if from_stdin {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf).map_err(|e| {
            powerfs_common::error::PowerFsError::Internal(format!("Failed to read stdin: {}", e))
        })?;
        Ok(buf)
    } else {
        Ok(decode_inline(&data.unwrap_or_default(), format))
    }
}

fn decode_inline(s: &str, format: &str) -> Vec<u8> {
    match format {
        "bytes" => s.as_bytes().to_vec(),
        "hex" => decode_hex(s),
        "base64" => decode_base64(s),
        other => {
            eprintln!("Unknown format: {}", other);
            std::process::exit(1);
        }
    }
}

/// Write a value: raw bytes to a file, or encoded to stdout.
fn write_value(
    value: &[u8],
    format: &str,
    output: Option<PathBuf>,
) -> Result<(), powerfs_common::error::PowerFsError> {
    if let Some(path) = output {
        std::fs::write(&path, value).map_err(|e| {
            powerfs_common::error::PowerFsError::Internal(format!(
                "Cannot write file '{}': {}",
                path.display(),
                e
            ))
        })
    } else {
        match format {
            "bytes" => std::io::stdout().write_all(value).map_err(|e| {
                powerfs_common::error::PowerFsError::Internal(format!(
                    "Failed to write stdout: {}",
                    e
                ))
            }),
            "hex" => {
                println!("{}", encode_hex(value));
                Ok(())
            }
            "base64" => {
                println!("{}", encode_base64(value));
                Ok(())
            }
            other => Err(powerfs_common::error::PowerFsError::Internal(format!(
                "Unknown format: {}",
                other
            ))),
        }
    }
}

fn encode_format(value: &[u8], format: &str) -> String {
    match format {
        "bytes" => String::from_utf8_lossy(value).into_owned(),
        "hex" => encode_hex(value),
        "base64" => encode_base64(value),
        other => {
            eprintln!("Unknown format: {}", other);
            std::process::exit(1);
        }
    }
}

/// Confirm a destructive action. With `--yes` it passes immediately; in a
/// terminal it prompts; non-interactively without `--yes` it refuses.
fn confirm_destructive(action: &str, target: &str, yes: bool) -> bool {
    if yes {
        return true;
    }

    if std::io::stdin().is_terminal() {
        eprint!("About to {} {}. Type 'yes' to continue: ", action, target);
        let mut line = String::new();
        if std::io::stdin().read_line(&mut line).is_ok() && line.trim() == "yes" {
            return true;
        }
        false
    } else {
        eprintln!(
            "Refusing to {} {} non-interactively without --yes",
            action, target
        );
        false
    }
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

fn encode_base64(bytes: &[u8]) -> String {
    let table: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        .chars()
        .collect();
    let mut s = String::new();

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        s.push(table[((triple >> 18) & 0x3F) as usize]);
        s.push(table[((triple >> 12) & 0x3F) as usize]);

        if chunk.len() > 1 {
            s.push(table[((triple >> 6) & 0x3F) as usize]);
        } else {
            s.push('=');
        }
        if chunk.len() > 2 {
            s.push(table[(triple & 0x3F) as usize]);
        } else {
            s.push('=');
        }
    }

    s
}
