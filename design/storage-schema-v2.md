# PowerFS 存储架构设计 V2

## 一、设计背景

当前 PowerFS 使用完整路径作为 key 存储文件元数据，存在以下问题：
1. rename 操作需要更新所有子条目，效率低下
2. list_entries 使用前缀匹配，容易误匹配深层路径
3. 缺少对多 Volume、条带化、S3/KV 后端的支持

本设计采用 Inode + 目录条目分离的存储架构，解决上述问题。

## 二、存储结构设计

### 2.1 Inode 条目

**Key**: `inode:{ino}`

```protobuf
message InodeEntry {
    uint64 ino = 1;                       // inode 号
    string name = 2;                      // 条目名称
    uint64 parent_ino = 3;                // 父目录 inode
    FuseAttributes attributes = 4;        // FUSE 属性
    repeated FileChunk chunks = 5;        // 文件块元数据
    string symlink_target = 6;            // 符号链接目标路径
    string hard_link_id = 7;              // 硬链接组 ID
    uint32 hard_link_counter = 8;         // 硬链接计数
    uint64 generation = 9;                // 代次号
    
    map<string, bytes> extended = 10;     // 扩展属性
    uint64 content_size = 11;             // 内容大小
    uint64 disk_size = 12;                // 磁盘大小
    string ttl = 13;                      // TTL
    string owner = 14;                    // 归属用户 ID
    
    StorageBackend backend = 15;          // 存储后端类型
    S3Location s3_location = 16;          // S3 位置
    KVLocation kv_location = 17;          // KV 位置
    StripeConfig stripe_config = 18;      // 条带配置
}
```

### 2.2 目录条目

**Key**: `dir:{parent_ino}:{name}`

```protobuf
message DirEntry {
    uint64 parent_ino = 1;                // 父目录 inode
    string name = 2;                      // 子条目名称
    uint64 child_ino = 3;                 // 子条目 inode
    uint32 child_type = 4;                // 0=file, 1=dir, 2=symlink
    
    uint32 mode = 5;                      // 文件类型+权限
    uint64 size = 6;                      // 文件大小
    uint64 mtime = 7;                     // 修改时间
    uint32 nlink = 8;                     // 链接数
}
```

### 2.3 路径索引

**Key**: `path:{full_path}`

```protobuf
message PathIndexEntry {
    uint64 ino = 1;                       // 目标 inode
    uint64 parent_ino = 2;                // 父目录 inode
    uint64 generation = 3;                // 代次号
}
```

### 2.4 数据位置信息

```protobuf
message FileChunk {
    uint64 offset = 1;                    // 文件内偏移
    uint64 size = 2;                      // 块大小
    uint64 mtime = 3;                     // 修改时间
    uint32 crc32 = 4;                     // 校验和
    
    repeated ChunkLocation locations = 5; // 多副本位置
    repeated StripeInfo stripes = 6;      // 条带化信息
    StorageBackend backend = 7;           // 存储后端类型
}

message ChunkLocation {
    uint32 volume_id = 1;                 // Volume ID
    uint64 file_key = 2;                  // 文件 key
    uint32 cookie = 3;                    // Cookie
    string fid = 4;                       // 完整 FID
    string data_center = 5;               // 数据中心
    string rack = 6;                      // 机架
    string node_address = 7;              // 节点地址
}

message StripeInfo {
    uint32 stripe_index = 1;              // 条带索引
    uint64 stripe_offset = 2;             // 条带内偏移
    uint64 stripe_size = 3;               // 条带大小
    uint32 volume_id = 4;                 // 条带所在 volume
    uint64 file_key = 5;                  // 条带文件 key
}

message StripeConfig {
    uint64 stripe_size = 1;               // 条带大小，缺省=volume_size，必须是volume_size的倍数
    uint32 stripe_count = 2;              // 条带数量
}

enum StorageBackend {
    POWERFS_VOLUME = 0;                   // PowerFS Volume
    S3 = 1;                               // S3 对象存储
    KV = 2;                               // KV 存储
    LOCAL_DISK = 3;                       // 本地磁盘
}

message S3Location {
    string bucket = 1;                    // S3 bucket
    string key = 2;                       // S3 key
    string region = 3;                    // 区域
    uint64 part_size = 4;                 // 分段大小
    repeated string part_etags = 5;       // 各分段 ETag
    string upload_id = 6;                 // 上传 ID
    uint64 total_parts = 7;               // 总分段数
}

message KVLocation {
    string table = 1;                     // KV 表名
    bytes key = 2;                        // KV key
    uint32 crc32 = 3;                     // 预留字段
}
```

## 三、核心操作流程

### 3.1 lookup(parent_ino, name)

```
1. 构造 key: "dir:{parent_ino}:{name}"
2. 查 DB → 获取 DirEntry(child_ino)
3. 查 "inode:{child_ino}" → 返回完整属性
```

### 3.2 list_entries(parent_ino)

```
1. 扫描所有以 "dir:{parent_ino}:" 开头的 key
2. 返回所有子条目列表（含缓存属性）
```

### 3.3 mkdir(parent_ino, name)

```
1. allocate_inode() → ino
2. put("inode:{ino}", InodeEntry(name, parent_ino, type=dir))
3. put("dir:{parent_ino}:{name}", DirEntry(child_ino=ino, child_type=dir))
4. put("path:{full_path}", PathIndexEntry(ino, parent_ino))
```

### 3.4 create(parent_ino, name)

```
1. allocate_inode() → ino
2. Assign volume → 获取 locations
3. put("inode:{ino}", InodeEntry(name, parent_ino, type=file, chunks=[...]))
4. put("dir:{parent_ino}:{name}", DirEntry(child_ino=ino, child_type=file))
5. put("path:{full_path}", PathIndexEntry(ino, parent_ino))
```

### 3.5 rename(parent_ino, old_name, new_name)

```
1. 查 "dir:{parent_ino}:{old_name}" → ino
2. 更新 "inode:{ino}" 的 name 为 new_name
3. delete("dir:{parent_ino}:{old_name}")
4. put("dir:{parent_ino}:{new_name}", DirEntry(child_ino=ino))
5. delete("path:{old_full_path}")
6. put("path:{new_full_path}", PathIndexEntry(ino, parent_ino))
```

### 3.6 chmod/chown(ino, mode/uid/gid)

```
1. 更新 "inode:{ino}" 的 attributes
2. 查 "inode:{ino}" 获取 parent_ino 和 name
3. 更新 "dir:{parent_ino}:{name}" 的 mode/uid/gid
```

## 四、目录条目缓存同步策略

所有属性变更必须同步更新目录条目缓存：

| 变更操作 | 更新 inode | 更新目录条目 |
|---------|-----------|-------------|
| chmod | mode | mode |
| chown | uid, gid | uid, gid |
| write/truncate | size, mtime | size, mtime |
| utime | atime, mtime | mtime |
| rename | name | 删除旧条目，添加新条目 |
| link/unlink | nlink | nlink |

## 五、实现优先级

### Phase 1: 核心存储结构（FUSE 基础功能）

- Inode 条目存储 (inode:{ino})
- 目录条目存储 (dir:{parent_ino}:{name})
- FUSE lookup 改为用 inode
- mkdir/create/delete/list_entries 基本操作
- 目录条目缓存同步

### Phase 2: 路径索引与通用 API

- 路径→inode 索引 (path:{full_path})
- GetEntryByPath API
- DeleteEntryByPath API
- S3/NFS 协议适配层基础

### Phase 3: 数据位置信息

- FileChunk 扩展支持多副本
- Stripe 支持
- StorageBackend 枚举
- S3Location + KVLocation

### Phase 4: 优化

- 内存 LRU 缓存
- 目录条目热度分层
- 批量操作优化
