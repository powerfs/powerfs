# PowerFS Lock Wire Protocol (TLV)

> Status: Reference spec (not committed — `docs/` is in `.gitignore`).
> Authority: This document is the single source of truth for the lock
> wire format. The Rust side (`powerfs-lock-net`) and the C kernel
> client (`powerfs-kernel`) both implement this spec independently.
> When the protocol changes, update this doc first, then both ends.
> Related: `docs/lock-optimization-plan.md` §3.1 (decision 1: protocol
> is a spec doc, not shared code).

## 1. Frame Layout

Every lock message is framed as:

```text
+----------+-------------------+----------------------+
| msg_type | payload_len (u32) | payload (bytes)      |
| 1 byte   | 4 bytes LE        | payload_len bytes    |
+----------+-------------------+----------------------+
```

- All multi-byte integers are **little-endian**.
- `payload_len` is a `u32` (max ~4 GiB; in practice a few hundred bytes).
- Receivers MUST tolerate trailing bytes after `payload_len` bytes
  (forward compatibility: future field additions append new TLV fields
  inside the payload; old receivers ignore unknown tags).

## 2. Payload: TLV Field Sequence

The payload is a sequence of Type-Length-Value fields, in arbitrary order:

```text
+----------+----------------+---------------------+
| tag (u8) | len (u32 LE)   | value (len bytes)   |
+----------+----------------+---------------------+
```

- Receivers MUST look up fields by tag, not by position.
- Each tag MUST appear at most once per message. Duplicate tags are a
  decode error (`DuplicateField`).
- Unknown tags MUST be ignored (forward compat).
- String values are UTF-8 without a NUL terminator.
- `len == 0` is valid for empty strings (e.g., `token=""` on a failed
  Grant).

## 3. Field Tags

| Tag    | Name          | Value encoding                        | Notes |
|--------|---------------|---------------------------------------|-------|
| `0x01` | inode         | `u64` LE (8 bytes)                    | Required in every message that targets a specific inode. |
| `0x02` | token         | UTF-8 string                          | Opaque lease token; may be empty on failed Grant. |
| `0x03` | mode          | `u8` (1 byte)                         | See §4. |
| `0x04` | range_start   | `u64` LE (8 bytes)                    | Present iff the lock is range-level. |
| `0x05` | range_end     | `u64` LE (8 bytes)                    | Present iff range is bounded; **absent = EOF**. Sentinels (`0xFFFFFFFFFFFFFFFF`) are tolerated but discouraged — prefer omitting the field. |
| `0x06` | timeout_ms    | `u64` LE (8 bytes)                    | Requested/granted lease duration in milliseconds. |
| `0x07` | sn            | `u64` LE (8 bytes)                    | Global sequence number (Early Grant). **MAY be omitted** when `sn == 0` (modularization phase). Receivers MUST default absent SN to `0`. |
| `0x08` | lease_ms      | `u64` LE (8 bytes)                    | Granted lease duration in Grant/RenewAck. |
| `0x09` | client_id     | UTF-8 string                          | Holder identity (FUSE mount ID or kernel client UUID). |
| `0x0A` | error_code    | `u8` (1 byte)                         | See §5. |

## 4. Mode Byte (`0x03`)

| Value | Name        | Meaning |
|-------|-------------|---------|
| `0x00`| `Shared`    | Read shared; multiple holders allowed on non-overlapping ranges. |
| `0x01`| `Exclusive` | Write exclusive; no other holder on same inode/range. |
| `0x02`| `Range`     | flock/OFD-style range write. Requires `range_start` (and `range_end` if bounded). |
| other | —           | Reserved. Receivers return `InvalidMode`. |

For `Range` mode, the `range_start`/`range_end` fields carry the range.
For `Shared`/`Exclusive` mode, the range fields are present iff the
request is range-level (routed to Volume); absent for inode-level
(routed to Filer).

## 5. Error Codes (`0x0A`)

| Value | Name                  | Maps to `LockError` variant |
|-------|-----------------------|-----------------------------|
| `0x00`| `OK`                  | (success, no error) |
| `0x01`| `NOT_FOUND`           | `NotFound` |
| `0x02`| `HOLDER_MISMATCH`     | `HolderMismatch` (context lost on wire) |
| `0x03`| `EXPIRED`             | `Expired` |
| `0x04`| `EXPIRED_BEYOND_GRACE`| `ExpiredBeyondGrace` |
| `0x05`| `CONFLICT`            | `Conflict` (context lost on wire) |
| `0x06`| `KEY_NOT_COVERED`     | `KeyNotCovered` |
| `0x07`| `QUARANTINED`         | `Quarantined` (client in fault-isolation pool — §8.2 Layer 3) |
| `0x08`| `NETWORK`             | `Network` |
| `0x09`| `INTERNAL`             | `Internal` |
| other | —                     | Receivers return `Internal("unknown error code: N")`. |

## 6. Message Types (`msg_type`)

| `msg_type` | Name          | Direction          | Required fields | Optional fields |
|------------|---------------|--------------------|------------------|-----------------|
| `0x01`     | `Acquire`     | client → server    | `inode`, `mode`, `timeout_ms`, `client_id` | `range_start`, `range_end` (range-level only) |
| `0x02`     | `Grant`       | server → client    | `inode`, `token`, `lease_ms`, `mode`, `error_code` | `sn` (omitted when 0), `range_start`, `range_end` |
| `0x03`     | `Release`     | client → server    | `inode`, `token`, `client_id` | — |
| `0x04`     | `ReleaseAck`  | server → client    | `inode`, `error_code` | — |
| `0x05`     | `Renew`       | client → server    | `inode`, `token`, `timeout_ms`, `client_id` | — |
| `0x06`     | `RenewAck`    | server → client    | `inode`, `lease_ms`, `error_code` | — |
| `0x07`     | `Revoke`      | server → client    | `inode`, `token` | — |
| `0x08`     | `Invalidate`  | server → client    | `inode` | `range_start`, `range_end` (full-inode invalidate if absent) |
| `0x09`     | `RevokeAck`   | client → server    | `inode`, `token`, `client_id` | — |
| other      | —             | —                  | Receivers return `InvalidMsgType`. | 

### 6.1 Routing

- `Acquire` with no `range_start` field and `mode ∈ {Shared, Exclusive}`
  → routed to Filer's `InodeLeaseStore` (inode-level).
- `Acquire` with `range_start` present OR `mode == Range`
  → routed to Volume's `RangeLeaseStore` (range-level).
- The two backends are mutually exclusive at runtime (client config
  `lease_mode = "inode" | "range"`), so a single Acquire never splits
  across both servers.

### 6.2 Grant Semantics

- Success: `error_code = OK`, `token` non-empty, `lease_ms > 0`.
- Failure: `error_code != OK`, `token` may be empty, `lease_ms = 0`,
  `sn = 0`. The client maps the code to a `LockError` via §5.
  Failures carry no lease — the client must retry or back off.

### 6.3 Revoke Lifecycle (§5.2 Early Revoke)

1. Server decides to evict a held lease early (another client queued).
2. Server → client: `Revoke { inode, token }`.
3. Client flushes dirty data covered by the lease, then:
4. Client → server: `RevokeAck { inode, token, client_id }`.
5. Server grants the next queued client (Early Grant — §5.2).
6. If no `RevokeAck` within the server's revocation timeout (§8.3:
   2 seconds), server marks the client `unresponsive`, force-reclaims
   the lease, and deducts the client's health score.

## 7. Transport Channel

Lock messages travel on `CHANNEL_LOCK` (§8.4), a dedicated logical
channel on the existing `powerfs-net` TCP connection. It has:

- A separate receive queue and worker pool (default 4 threads) so
  high IO load cannot starve lock messages.
- Independent rate-limit config (not affected by IO throttling).
- Optional dedicated TCP connection when
  `lock_dedicated_connection = true` (§8.4 scheme B).

Within `CHANNEL_LOCK`, messages are further prioritized (§8.5):

| Priority | Messages |
|----------|----------|
| `P0` (highest) | `Revoke`, `RevokeAck` |
| `P1` | `Grant` |
| `P2` | `Acquire` |
| `P3` | `Renew`, `Release` |

## 8. Worked Example: Acquire → Grant

Client requests an exclusive inode-level lock on inode 42 for 30 s:

```text
Frame:
  msg_type    = 0x01 (Acquire)
  payload_len = 0x1D (29 bytes)
Payload (TLV):
  0x01 0x04 0x00 0x00 0x00 0x2A 0x00 0x00 0x00   # inode=42
  0x03 0x01 0x01                                   # mode=Exclusive
  0x06 0x08 0x30 0x75 0x00 0x00 0x00 0x00 0x00    # timeout_ms=30000
  0x09 0x08 0x63 0x6C 0x69 0x65 0x6E 0x74 0x2D 0x41 # client_id="client-A"
```

Server grants:

```text
Frame:
  msg_type    = 0x02 (Grant)
  payload_len = ...
Payload:
  0x01 ... (inode=42)
  0x02 ... (token="lease-0-abc")
  0x08 ... (lease_ms=30000)
  0x03 0x01 0x01 (mode=Exclusive)
  0x0A 0x01 0x00 (error_code=OK)
  # sn omitted (sn=0 in modularization phase)
```

## 9. Versioning & Compatibility

- **Field additions** are forward-compatible: new optional fields use
  new tags; old receivers ignore them.
- **New message types** use unused `msg_type` bytes; old receivers
  return `InvalidMsgType` and the caller falls back.
- **Field removal** is not supported — once a tag is assigned, it stays.
  Obsolete fields may be left empty (`len=0`).
- The `sn` field's optional encoding (`sn=0` omitted) is the canonical
  example: during the modularization phase SN is always 0, so the field
  is omitted; once the optimization phase fills it in, it appears.
