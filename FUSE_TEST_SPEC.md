# PowerFS FUSE 测试规范

## 一、测试环境准备

### 1.1 启动集群
```bash
docker/scripts/start-cluster.sh
```

### 1.2 启动 FUSE 客户端
```bash
docker/scripts/start-fuse.sh
```

### 1.3 验证环境
```bash
docker exec fuse-1 ls /mnt/powerfs
```

## 二、测试顺序（从简单到复杂）

### Phase 1: 文件基础操作（open/close/unlink）

#### Test 1.1: 创建文件
- 创建空文件
- 验证文件存在
- 删除文件
- 验证文件不存在

#### Test 1.2: 打开/关闭文件
- 创建文件
- 打开文件
- 关闭文件
- 删除文件

#### Test 1.3: 删除不存在的文件
- 尝试删除不存在的文件
- 验证返回错误

### Phase 2: 目录操作（mkdir/readdir/rmdir）

#### Test 2.1: 创建目录
- 创建目录
- 验证目录存在
- 删除目录
- 验证目录不存在

#### Test 2.2: 读取目录内容
- 创建目录
- 在目录中创建文件
- 读取目录内容
- 验证文件列表正确
- 删除文件和目录

#### Test 2.3: 删除非空目录
- 创建目录
- 在目录中创建文件
- 尝试删除非空目录
- 验证返回错误
- 删除文件后再删除目录

### Phase 3: 文件读写操作（write/read）

#### Test 3.1: 写入并读取小文件
- 创建文件
- 写入少量数据（< 16MB）
- 关闭文件
- 重新打开文件
- 读取文件内容
- 验证内容匹配

#### Test 3.2: 文件大小验证
- 创建文件
- 写入数据
- 获取文件大小
- 验证大小正确

#### Test 3.3: 追加写入
- 创建文件并写入初始数据
- 追加写入更多数据
- 验证总内容正确

### Phase 4: 多级目录结构

#### Test 4.1: 多级目录创建和遍历
- 创建目录结构: level1/level2/level3
- 在最底层目录创建文件
- 验证整个结构存在
- 逐层删除

#### Test 4.2: 多级目录文件操作
- 创建多级目录
- 在不同层级创建文件
- 读取所有文件内容
- 验证正确性

## 三、测试执行

### 3.1 运行测试
```bash
# 在 fuse-1 容器中运行
docker exec fuse-1 bash -c "POWERFS_MOUNT=/mnt/powerfs /app/target/debug/deps/fuse_basic_test-xxx --test-threads=1"
```

### 3.2 单独运行某个测试
```bash
docker exec fuse-1 bash -c "POWERFS_MOUNT=/mnt/powerfs /app/target/debug/deps/fuse_basic_test-xxx test_create_file_open_close_unlink"
```

## 四、故障排查

### 4.1 查看 FUSE 日志
```bash
docker logs fuse-1 2>&1 | tail -100
```

### 4.2 查看挂载状态
```bash
docker exec fuse-1 cat /proc/mounts | grep fuse
```

### 4.3 重启 FUSE 服务
```bash
docker compose -f docker/docker-compose.yml restart fuse-1
```

### 4.4 重建 Docker 镜像
```bash
docker/scripts/start-cluster.sh --build
docker/scripts/start-fuse.sh --build
```

## 五、测试通过标准

所有测试用例必须：
1. 运行完成（不卡死）
2. 断言通过（无失败）
3. 资源正确清理（无残留文件/目录）