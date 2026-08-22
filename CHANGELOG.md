# Changelog

## [Unreleased] - 2026-08-22

### Fixed - T1.3 Cross-Client 4MB Flat Read EIO

Four bugs identified and fixed in the T1.3 cross-client data persistence
test scenario (fuse-1 writes 4MB Flat file, fuse-2 cross-client read
returns EIO).

#### Bug 1: Filer broadcasts Invalidate for chunk files
- **Component**: powerfs-filer (net_handler.rs), powerfs-fuse (invalidate_handler.rs)
- **Root cause**: `handle_update_inode_size_chunks` unconditionally called
  `notify_inode_change` for all files. For chunk files, the writer's own
  chunk_cache was cleared via InvalidateHandler eviction
  (`chunk_cache.remove_inode_chunks(inode)`).
- **Fix**: Only inline files (`inline_data.is_some()`) broadcast Invalidate.
  Chunk files skip broadcast, aligning with Ceph MDS cap recall model where
  close does not evict the writer's own page cache.

#### Bug 2: FUSE release skips sync for O_RDONLY opens with dirty chunks
- **Component**: powerfs-fuse (fuse.rs: release)
- **Root cause**: Release unconditionally skipped
  `sync_size_chunks_on_close` for `O_RDONLY` opens. But kernel writeback
  (`dirty_writeback_centisecs`) may emit FUSE_WRITE during a read-only
  open's lifetime. Those dirty chunks were flushed to Volume Server, but
  metadata was not synced to Filer → cross-client readers saw size=0.
- **Fix**: Capture `had_dirty = chunk_cache.has_dirty_chunks(inode)` before
  flush. Force sync when dirty chunks were flushed, regardless of open flags.

#### Bug 3: Filer async_meta_persist doesn't update MetaCache projected state
- **Component**: powerfs-filer (meta_cache.rs, meta_shard_manager.rs)
- **Root cause**: In async_meta_persist mode, `update_inode_size_chunks_atomic`
  proposed Raft log but did not update MetaCache. Cross-client `getattr` hit
  MetaCache (create-time staged value: size=0, chunks=[]) and returned stale
  data. Reader's `READ_LAG` spin-wait timed out after 10s → EIO.
- **Fix**: New `project_update_size_chunks` method on MetaCache updates
  size/chunks/inline_data immediately after `propose_meta`, mirroring Ceph
  MDS projected state model: memory updates precede journal apply.

#### Bug 4: attr_to_cached_entry treats Placement::Flat as Stripe
- **Component**: powerfs-fuse (fuse.rs: attr_to_cached_entry)
- **Root cause**: Used `attr.placement.is_some()` to determine Stripe
  layout. But Filer's `detect_placement_from_chunks` returns
  `Some(Placement::Flat)` (not None) for single-volume files. Flat files
  got `fid=None` → Flat read path hit `fid.ok_or(EIO)?` → EIO.
- **Fix**: Use `matches!` to suppress fid only for `Placement::Stripe`
  and `Placement::WideStripe`. Flat files reconstruct fid from
  volume_id/file_key or chunks[0] normally.

### Fixed - Filer Failover (Single-Point Failure)

#### Bug 5: Filer single-point failure blocks all shard requests
- **Component**: powerfs-fuse-core (topology.rs, meta_shard_client.rs, fuse_client_facade.rs)
- **Root cause**: MetaShardClient's rotation candidates only contained one
  filer address (the first from Master topology). When that filer crashed,
  all shard requests blocked — even though other filers were healthy.
- **Fix**: `ClusterTopology` now stores `all_filer_addresses` (every
  healthy filer from Master). MetaShardClient uses this as rotation list.
  REDIRECT handling bumps `cache_epoch` and switches to new leader.

### Fixed - Cache Invalidation on Leader Switch

#### Bug 6: invalidate_all doesn't clear cap fields on Dirty/Flushing entries
- **Component**: powerfs-fuse (cache.rs)
- **Root cause**: On leader switch, `invalidate_all` skipped Dirty/Flushing
  entries entirely, leaving stale `cap` fields. After re-acquiring caps from
  the new leader, old cap seq numbers caused visibility issues.
- **Fix**: Unconditionally clear `cap` field on all entries (including
  Dirty/Flushing) during `invalidate_all`, while preserving dirty data.

### Verification

- T1.3: Two 4MB cross-client read tests pass with matching md5sum
- Commit: a8f4813f

### Key Lessons

1. **MetaCache projected state is mandatory**: In async_meta_persist mode,
   any mutation proposed to Raft must also update MetaCache immediately.
   Otherwise, `get_inode` returns stale pre-propose values. This mirrors
   Ceph MDS where projected (memory) state precedes journal apply.

2. **Placement enum discrimination**: `Option<Placement>.is_some()` is
   not equivalent to "is Stripe". Flat is a valid placement variant.
   Always use `matches!` with specific variants.

3. **Invalidate broadcast scope**: Only inline files (data in Filer
   metadata) need broadcast Invalidate. Chunk files (data in Volume
   Server) don't need it — readers fetch via getattr TTL expiry.
   Broadcasting for chunk files harms the writer's own chunk_cache.

4. **Release sync must consider kernel writeback**: Open flags alone
   don't determine whether dirty data exists. Kernel writeback can
   flush during any open mode. Always check `has_dirty_chunks` before
   deciding to skip sync.
