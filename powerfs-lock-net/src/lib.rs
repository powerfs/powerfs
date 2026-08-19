//! powerfs-lock-net: Wire protocol layer for PowerFS lock messages.
//!
//! This crate implements the TLV (Type-Length-Value) codec for lock
//! protocol messages defined in `docs/lock-protocol.md`. It is the Rust
//! counterpart of the byte-level spec; the in-kernel C client implements
//! the same wire format independently (see `docs/lock-protocol.md`).
//!
//! # Wire format (see docs/lock-protocol.md for the authoritative spec)
//!
//! ```text
//! Frame:     [msg_type:u8][payload_len:u32 LE][payload: bytes]
//! Payload:   sequence of TLV fields
//! Field:     [field_tag:u8][field_len:u32 LE][field_value: bytes]
//! ```
//!
//! Integers are little-endian. Strings are UTF-8. Field order is not
//! significant — receivers must look up by tag, not by position.
//!
//! # Architecture (docs/lock-optimization-plan.md §3.1, decision 1)
//!
//! - Rust end (this crate + `powerfs-lock-fuse`): uses these functions.
//! - C end (`powerfs-kernel`): independent pure-C implementation of the
//!   same byte format (a few hundred lines), kept in sync via the doc.

pub mod codec;
pub mod error;
pub mod msg;

pub use codec::{decode_frame, encode_frame};
pub use error::CodecError;
pub use msg::{ErrorCode, FieldTag, Frame, Message, MsgType, RANGE_END_EOF_SENTINEL};
