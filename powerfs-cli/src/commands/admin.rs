//! `powerfs-cli admin` — cluster administration helpers.
//!
//! Pure-local commands (no master API call) that assist with cluster
//! bootstrap. Currently provides `generate-token` to create a
//! cryptographically secure random admin token for use in `master-*.toml`.
//!
//! # Why local-only
//!
//! The `admin_token` field is a cluster-wide shared constant: every master
//! verifies the bearer token locally with constant-time comparison, so
//! there is no Raft-replicated state to write. Generating the value
//! centrally and copying it into each master's config file is the simplest
//! reliable approach and keeps the configuration file as the single source
//! of truth (per project convention: "init tools must use a unified config
//! file").
//!
//! # Usage
//!
//! ```sh
//! powerfs-cli admin generate-token                       # 32-byte base64url
//! powerfs-cli admin generate-token -n 48                 # 48-byte base64url
//! powerfs-cli admin generate-token --encoding hex        # 32-byte hex
//! TOKEN=$(powerfs-cli admin generate-token)              # capture in shell
//! ```
//!
//! After generation, copy the printed value into the `admin_token` field
//! of every `master-*.toml` in the cluster — all masters MUST share the
//! same token value.

use clap::{Args, Subcommand};
use rand::rngs::OsRng;
use rand::RngCore;

use powerfs_common::error::{PowerFsError, Result};

/// Admin management subcommands. Pure-local helpers — no master network
/// round-trip, safe to run offline.
#[derive(Subcommand, Debug)]
pub enum AdminSubcommand {
    /// Generate a cryptographically secure random admin token using
    /// OsRng (CSPRNG) and print it to stdout.
    ///
    /// The token is NOT written to any file and NOT sent to any master.
    /// Copy the printed value into the `admin_token` field of every
    /// `master-*.toml` in your cluster — all masters must share the
    /// same value because each master verifies the bearer token locally
    /// with constant-time comparison.
    GenerateToken(GenerateTokenArgs),
}

#[derive(Args, Debug)]
pub struct GenerateTokenArgs {
    /// Number of random bytes to encode. Default 32 (256-bit entropy).
    /// Minimum 16 (128-bit) to prevent weak tokens. There is no hard
    /// upper bound, but values above 64 yield diminishing returns.
    #[arg(short = 'n', long, default_value_t = 32)]
    length: usize,

    /// Output encoding: `base64url` (default, URL-safe, no padding) or
    /// `hex` (lowercase). base64url is more compact and survives
    /// shell/URL contexts without escaping; hex is human-friendly and
    /// pairs naturally with `openssl rand -hex`.
    #[arg(short, long, default_value = "base64url")]
    encoding: String,
}

/// Entry point dispatched from `main.rs`. All variants are pure local
/// computations — no async runtime required.
pub fn admin(command: AdminSubcommand) -> Result<()> {
    match command {
        AdminSubcommand::GenerateToken(args) => generate_token(args),
    }
}

fn generate_token(args: GenerateTokenArgs) -> Result<()> {
    if args.length < 16 {
        return Err(PowerFsError::InvalidRequest(format!(
            "token length {} is below the 16-byte (128-bit) minimum",
            args.length
        )));
    }
    if args.length > 256 {
        return Err(PowerFsError::InvalidRequest(format!(
            "token length {} exceeds the 256-byte cap (no security benefit, larger config)",
            args.length
        )));
    }

    let mut buf = vec![0u8; args.length];
    // OsRng pulls from the kernel CSPRNG (/dev/urandom or getrandom(2)).
    // It is the same source used for TLS key generation and is safe for
    // long-term secret material.
    OsRng.fill_bytes(&mut buf);

    let token = match args.encoding.as_str() {
        "base64url" => base64url_encode(&buf),
        "hex" => hex_encode(&buf),
        other => {
            return Err(PowerFsError::InvalidRequest(format!(
                "unknown encoding '{}': expected 'base64url' or 'hex'",
                other
            )));
        }
    };

    // Print ONLY the token to stdout so `$(powerfs-cli admin generate-token)`
    // captures a clean value with no surrounding text. Hint text goes to
    // stderr so it does not pollute capture.
    println!("{}", token);
    eprintln!(
        "Generated {}-byte {} token ({} chars). Copy this value into the",
        args.length,
        args.encoding,
        token.len()
    );
    eprintln!("`admin_token` field of every master-*.toml in your cluster.");
    eprintln!("All masters must share the same token value. Do NOT commit");
    eprintln!("the token to version control.");
    Ok(())
}

/// RFC 4648 base64url without padding (URL-safe, no '=' suffix).
/// Hand-rolled to avoid pulling in the `base64` crate as a new CLI
/// dependency — the encoding table is fixed and the algorithm is trivial.
fn base64url_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((bytes.len() * 4).div_ceil(3));
    let (chunks, rem) = bytes.as_chunks::<3>();
    for c in chunks {
        let n = ((c[0] as u32) << 16) | ((c[1] as u32) << 8) | (c[2] as u32);
        out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        out.push(TABLE[(n & 0x3F) as usize] as char);
    }
    match rem.len() {
        1 => {
            let n = (rem[0] as u32) << 16;
            out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
            // No padding per base64url spec.
        }
        2 => {
            let n = ((rem[0] as u32) << 16) | ((rem[1] as u32) << 8);
            out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
            out.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        }
        _ => {}
    }
    out
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_known_vectors() {
        // RFC 4648 §5 / §10 (URL-safe variant, no padding).
        assert_eq!(base64url_encode(b""), "");
        assert_eq!(base64url_encode(b"f"), "Zg");
        assert_eq!(base64url_encode(b"fo"), "Zm8");
        assert_eq!(base64url_encode(b"foo"), "Zm9v");
        assert_eq!(base64url_encode(b"foob"), "Zm9vYg");
        assert_eq!(base64url_encode(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn hex_known_vectors() {
        assert_eq!(hex_encode(b""), "");
        assert_eq!(hex_encode(b"\x00"), "00");
        assert_eq!(hex_encode(b"\xff"), "ff");
        assert_eq!(hex_encode(b"PowerFS"), "506f7765724653");
    }

    #[test]
    fn generate_token_rejects_short_length() {
        let args = GenerateTokenArgs {
            length: 8,
            encoding: "base64url".into(),
        };
        assert!(generate_token(args).is_err());
    }

    #[test]
    fn generate_token_rejects_huge_length() {
        let args = GenerateTokenArgs {
            length: 1024,
            encoding: "base64url".into(),
        };
        assert!(generate_token(args).is_err());
    }

    #[test]
    fn generate_token_rejects_unknown_encoding() {
        let args = GenerateTokenArgs {
            length: 32,
            encoding: "rot13".into(),
        };
        assert!(generate_token(args).is_err());
    }
}
