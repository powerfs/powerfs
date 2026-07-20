# FUSE Coherence v2 Missing Features - Enterprise Implementation Plan

## Repo Research Conclusion

Based on codebase analysis of `powerfs-fuse-enterprise`:

**Architecture Change**: Enterprise has moved to OR-Set weak consistency model. Leases are deprecated per [fuse-cache-architecture.md](file:///home/portion/powerfs/powerfs-fuse-enterprise/design/fuse-cache-architecture.md)

**Already Implemented**:
- `cache.rs`: `clear_all()` for batch invalidation, `path_generations`, `update_path_generation()`, `get_path_generation()` ✓
- Server-side: `JOB_COMPLETE` event, `job_id` field in proto ✓

**Missing Implementation**:
1. **Feature 2**: Handle `JOB_COMPLETE` notifications for batch invalidation
2. **Feature 3**: Generation validation in lookup
3. **Feature 4**: Job-level filtering with `job_id` (simplified for OR-Set model)
4. **Feature 1**: Lease auto-renewal - **Deprecated** in enterprise OR-Set architecture

## Files to Edit

| File | Changes |
|------|---------|
| `powerfs-fuse-enterprise/src/fuse.rs` | Add metadata subscription loop with JOB_COMPLETE handling |
| `powerfs-fuse-enterprise/src/fuse.rs` | Add generation validation in lookup |
| `powerfs-fuse-enterprise/src/fuse.rs` | Add `job_id` support from environment variable |
| `powerfs-fuse-enterprise/src/cache.rs` | Add generation validation helper methods |

## Implementation Steps

### Step 1: Feature 2 - Job Completion Batch Invalidation (P1)

**File: `powerfs-fuse-enterprise/src/fuse.rs`**

1. Add metadata subscription loop after FUSE mount (around line 101):
   ```rust
   let cache_clone = cache.clone();
   let sync_client_clone = sync_client.clone();
   tokio::spawn(async move {
       loop {
           match sync_client_clone.subscribe_metadata("/").await {
               Ok(mut stream) => {
                   while let Some(notification) = stream.message().await.unwrap_or(None) {
                       if notification.event_type == 4 { // JOB_COMPLETE
                           cache_clone.clear_all();
                           info!("JOB_COMPLETE received, cleared all cache");
                       } else {
                           // Update generation tracking
                           cache_clone.update_path_generation(&notification.path, notification.generation);
                       }
                   }
               }
               Err(e) => {
                   warn!("Metadata subscription failed: {}, reconnecting...", e);
                   tokio::time::sleep(Duration::from_secs(5)).await;
               }
           }
       }
   });
   ```

### Step 2: Feature 3 - Lookup Generation Validation (P2)

**File: `powerfs-fuse-enterprise/src/fuse.rs`**

1. Modify `lookup()` method to validate generation:
   ```rust
   fn lookup(&self, ctx: &Context, parent: u64, name: &CStr) -> Result<Entry> {
       let name_str = name.to_str().unwrap_or("");
       let path = self.cache.build_path(parent, name_str);
       
       // Check cache
       if let Some(entry) = self.cache.get_inode_by_path(&path) {
           // Validate generation
           if let Some(path_gen) = self.cache.get_path_generation(&path) {
               if entry.generation < path_gen {
                   // Generation expired, skip cache
                   debug!("Cache generation expired for {}, fetching from master", path);
               } else {
                   return Ok(self.create_entry(&entry));
               }
           } else {
               return Ok(self.create_entry(&entry));
           }
       }
       
       // Fetch from master
       // ... existing logic
   }
   ```

### Step 3: Feature 4 - Job-level Filtering (P2)

**File: `powerfs-fuse-enterprise/src/fuse.rs`**

1. Add `job_id` field to `FuseApp` struct:
   ```rust
   pub struct FuseApp {
       // ... existing fields
       job_id: String,
   }
   ```

2. Read `job_id` from environment variable in `new()`:
   ```rust
   let job_id = std::env::var("POWERFS_JOB_ID").unwrap_or_default();
   ```

3. Pass `job_id` to `PowerFsFs` and store it

4. Modify metadata subscription loop to filter by job_id:
   ```rust
   if notification.event_type == 4 { // JOB_COMPLETE
       // Only clear cache if job_id matches or client has no job_id
       let notification_job_id = notification.job_id.clone();
       if self.job_id.is_empty() || 
          notification_job_id.is_empty() || 
          self.job_id == notification_job_id {
           cache_clone.clear_all();
           info!("JOB_COMPLETE received for job {}, cleared all cache", notification_job_id);
       }
   }
   ```

### Step 4: Helper Methods in Cache

**File: `powerfs-fuse-enterprise/src/cache.rs`**

1. Add `build_path()` helper if not already present

## Verification Plan

```bash
# 1. Compilation
cargo check -p powerfs-fuse-enterprise

# 2. Format and clippy
cargo fmt --all
cargo clippy -p powerfs-fuse-enterprise --tests -- -D warnings

# 3. Run tests
cargo test --package powerfs-fuse-enterprise

# 4. Start FUSE environment
bash docker/scripts/start-fuse.sh -b

# 5. Test JOB_COMPLETE
# Trigger job completion via API and verify cache is cleared
```

## Risk Handling

1. **Notification Loss**: Metadata subscription loop automatically reconnects on failure
2. **Cache Clear Race**: `clear_all()` uses RwLock for thread safety
3. **Job ID Mismatch**: Log warnings when job_id filtering causes skipped invalidations
4. **Generation Validation Performance**: Minimal overhead - just a hash lookup