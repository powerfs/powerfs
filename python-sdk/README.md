# PowerFS Python SDK

PowerFS Python SDK 提供了用于管理和解决 PowerFS 文件系统中冲突的 Python API。

## 功能特性

- **冲突管理**：检测、查看和解决分布式文件系统中的冲突
- **策略管理**：配置目录级别的合并策略
- **批量操作**：支持批量检测和解决冲突
- **消息通知**：事件订阅、Webhook 管理和实时告警
- **事件驱动**：支持冲突事件订阅和自动处理

## 安装

```bash
# 从 PyPI 安装
pip install powerfs-sdk

# 从源码安装
git clone https://github.com/powerfs/powerfs-python-sdk.git
cd powerfs-python-sdk
pip install -e .
```

## 快速开始

```python
from powerfs import PowerFSClient, ConflictType, MergePolicy, ConflictResolution

# 初始化客户端
client = PowerFSClient(
    master_endpoint="http://localhost:9333",
    api_key="your-api-key"  # 可选
)

# 1. 获取冲突列表
conflicts = client.conflict_manager.get_conflicts()
print(f"发现 {len(conflicts)} 个未解决的冲突")

# 2. 逐个处理冲突
for conflict in conflicts:
    print(f"冲突 ID: {conflict.id}")
    print(f"冲突类型: {conflict.conflict_type.name}")
    
    # 根据冲突类型应用不同的解决策略
    if conflict.conflict_type == ConflictType.WRITE_WRITE:
        client.conflict_manager.resolve_conflict(
            conflict.id,
            ConflictResolution.KEEP_LAST  # 保留最后写入的版本
        )

# 3. 批量解决冲突
resolved = client.conflict_manager.batch_resolve(
    policy=MergePolicy.LWW,
    dir_path="/data/documents",
    recursive=True
)
print(f"批量解决了 {resolved} 个冲突")

# 4. 获取冲突统计
stats = client.conflict_manager.get_stats()
print(f"总冲突数: {stats.total_count}")
print(f"已解决: {stats.resolved_count}")
print(f"解决率: {stats.resolution_rate:.1f}%")
```

## 核心概念

### 冲突类型

| 类型 | 描述 | 典型场景 |
|------|------|----------|
| `CREATE_CREATE` | 同目录下创建同名文件 | 两个客户端同时创建 `/docs/readme.md` |
| `WRITE_WRITE` | 同一文件多次写入 | 两个客户端同时编辑同一文件 |
| `WRITE_UNLINK` | 写入已删除文件 | 客户端A删除文件，客户端B继续写入 |
| `DELETE_CREATE` | 删除后又创建 | 文件被删除后又重新创建 |
| `RENAME_CONFLICT` | 重命名冲突 | 两个客户端同时重命名同一文件 |

### 解决策略

| 策略 | 描述 | 适用场景 |
|------|------|----------|
| `KEEP_FIRST` | 保留第一个版本 | 需要确定性结果的场景 |
| `KEEP_LAST` | 保留最后版本 (LWW) | 大多数默认场景 |
| `KEEP_ALL` | 全部保留 (自动重命名) | 文档协作场景 |
| `MERGE` | 合并内容 | 文本文件协作 |

### 合并策略

| 策略 | 描述 | 特点 |
|------|------|------|
| `LWW` | 最后写入获胜 | 简单、高效 |
| `WRITE_PRIORITY` | 写操作优先 | 适合频繁写入场景 |
| `AGGRESSIVE` | 激进合并 | 最小化人工干预 |
| `CONSERVATIVE` | 保守策略 | 需要人工确认 |

### 事件类型

| 类型 | 描述 | 级别 |
|------|------|------|
| `CONFLICT_DETECTED` | 检测到新冲突 | WARNING |
| `CONFLICT_RESOLVED` | 冲突已解决 | INFO |
| `NODE_STATUS_CHANGED` | 节点状态变化 | 可变 |
| `VOLUME_STATUS_CHANGED` | 卷状态变化 | 可变 |
| `ALERT_TRIGGERED` | 触发告警 | ERROR/CRITICAL |
| `SESSION_CREATED` | 会话创建 | INFO |
| `SESSION_DELETED` | 会话删除 | INFO |

### 通知级别

| 级别 | 描述 | 颜色标识 |
|------|------|----------|
| `INFO` | 信息性消息 | 蓝色 |
| `WARNING` | 警告（需要关注） | 黄色 |
| `ERROR` | 错误（可能需要处理） | 红色 |
| `CRITICAL` | 严重（需要立即处理） | 红色闪烁 |

## API 参考

### PowerFSClient

```python
client = PowerFSClient(
    master_endpoint="http://localhost:9333",
    api_key="sk-xxx",
    timeout=30,
    retries=3
)

# 获取管理器
conflict_manager = client.conflict_manager
policy_manager = client.policy_manager
notification_manager = client.notification_manager
```

### ConflictManager

```python
# 获取冲突列表
conflicts = client.conflict_manager.get_conflicts(
    dir_path="/data",
    unresolved_only=True
)

# 解决单个冲突
success = client.conflict_manager.resolve_conflict(
    conflict_id="conflict-123",
    resolution=ConflictResolution.KEEP_LAST
)

# 批量检测冲突
stats = client.conflict_manager.batch_detect(
    dir_path="/data",
    recursive=True
)

# 批量解决冲突
count = client.conflict_manager.batch_resolve(
    policy=MergePolicy.LWW,
    dir_path="/data",
    recursive=True
)

# 获取冲突统计
stats = client.conflict_manager.get_stats()

# 批量忽略冲突
count = client.conflict_manager.batch_ignore(
    dir_path="/data"
)
```

### PolicyManager

```python
# 获取目录策略
policy = client.policy_manager.get_policy(dir_path="/data")

# 设置目录策略
success = client.policy_manager.set_policy(
    policy=MergePolicy.LWW,
    dir_path="/data",
    recursive=True
)

# 自动解决冲突
count = client.policy_manager.auto_resolve(
    policy=MergePolicy.LWW,
    dir_path="/data"
)
```

### NotificationManager

```python
# 订阅事件
def handle_conflict(event):
    print(f"检测到冲突: {event.event_id}")

client.notification_manager.subscribe(
    EventType.CONFLICT_DETECTED,
    handle_conflict
)

# 启动事件监听
client.notification_manager.start_listening(
    poll_interval=5  # 轮询间隔（秒）
)

# 获取历史事件
events = client.notification_manager.get_events(
    event_type=EventType.CONFLICT_DETECTED,
    level=NotificationLevel.WARNING,
    limit=50
)

# 创建 Webhook
webhook = client.notification_manager.create_webhook(
    name="冲突告警",
    url="https://your-server.com/webhooks/powerfs",
    events=[EventType.CONFLICT_DETECTED],
    level_filter=NotificationLevel.WARNING,
    secret="your-secret-key"
)

# 测试 Webhook
client.notification_manager.test_webhook(webhook.webhook_id)

# 停止事件监听
client.notification_manager.stop_listening()
```

## 消息通知使用指南

### 1. 事件订阅与实时监听

```python
from powerfs import PowerFSClient, EventType, NotificationLevel

# 初始化客户端
client = PowerFSClient(master_endpoint="http://localhost:9333")

# 定义事件处理器
def on_conflict_detected(event):
    """处理冲突检测事件"""
    print(f"🔔 冲突检测 [{event.level.value}]")
    print(f"   ID: {event.event_id}")
    print(f"   消息: {event.message}")
    print(f"   时间: {event.timestamp}")
    if event.payload:
        print(f"   详情: {event.payload}")

def on_alert_triggered(event):
    """处理告警事件"""
    print(f"🚨 告警触发 [{event.level.value}]")
    print(f"   ID: {event.event_id}")
    print(f"   来源: {event.source}")
    print(f"   消息: {event.message}")

# 订阅事件
client.notification_manager.subscribe(
    EventType.CONFLICT_DETECTED,
    on_conflict_detected
)
client.notification_manager.subscribe(
    EventType.ALERT_TRIGGERED,
    on_alert_triggered
)

# 启动后台监听
client.notification_manager.start_listening(poll_interval=5)

# 程序保持运行
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    client.notification_manager.stop_listening()
    client.close()
```

### 2. Webhook 配置

```python
from powerfs import PowerFSClient, EventType, NotificationLevel

client = PowerFSClient(master_endpoint="http://localhost:9333")

# 创建 Webhook
webhook = client.notification_manager.create_webhook(
    name="企业微信告警",
    url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx",
    events=[
        EventType.CONFLICT_DETECTED,
        EventType.ALERT_TRIGGERED,
        EventType.NODE_STATUS_CHANGED
    ],
    level_filter=NotificationLevel.WARNING,  # 只转发 WARNING 及以上级别
    secret="your-signing-secret",
    headers={"Content-Type": "application/json"},
    timeout=30,
    max_retries=3
)

print(f"Webhook 创建成功: {webhook.webhook_id}")

# 获取所有 Webhook 配置
webhooks = client.notification_manager.get_webhooks()
for w in webhooks:
    print(f"ID: {w.webhook_id}, 名称: {w.name}, 状态: {'启用' if w.enabled else '禁用'}")

# 测试 Webhook
if client.notification_manager.test_webhook(webhook.webhook_id):
    print("Webhook 测试成功")

# 更新 Webhook（禁用）
webhook = client.notification_manager.update_webhook(
    webhook_id=webhook.webhook_id,
    enabled=False
)

# 删除 Webhook
client.notification_manager.delete_webhook(webhook.webhook_id)
```

### 3. 查询 Webhook 投递记录

```python
# 查询投递记录
deliveries = client.notification_manager.get_webhook_deliveries(
    webhook_id=webhook.webhook_id,
    limit=100
)

for delivery in deliveries:
    print(f"投递 ID: {delivery.delivery_id}")
    print(f"  状态: {delivery.status}")
    print(f"  响应码: {delivery.response_code}")
    print(f"  响应时间: {delivery.response_time}ms")
    if delivery.error_message:
        print(f"  错误: {delivery.error_message}")
```

## 异常处理

```python
from powerfs.exceptions import (
    PowerFSException,
    ConnectionError,
    ConflictNotFound,
    ResolutionError,
    PolicyError
)

try:
    conflicts = client.conflict_manager.get_conflicts()
except ConnectionError as e:
    print(f"连接失败: {e}")
except ConflictNotFound as e:
    print(f"冲突不存在: {e}")
except PowerFSException as e:
    print(f"PowerFS 错误: {e}")
```

## Agent 集成

该 SDK 专为与 AI Agent 集成而设计，支持：

1. **定时检测**：定期扫描冲突并自动处理
2. **事件驱动**：订阅冲突事件并实时响应
3. **策略引擎**：根据业务规则自动选择解决策略
4. **人工审核**：对于复杂冲突支持提交人工审核
5. **告警通知**：通过 Webhook 或其他渠道发送告警

### 示例：自动冲突处理 Agent

```python
import time
from powerfs import PowerFSClient, ConflictType, ConflictResolution

class ConflictResolutionAgent:
    def __init__(self, master_endpoint):
        self.client = PowerFSClient(master_endpoint=master_endpoint)
    
    def run(self, interval=60):
        """定时运行冲突检测和处理"""
        while True:
            self.detect_and_resolve()
            time.sleep(interval)
    
    def detect_and_resolve(self):
        """检测并解决冲突"""
        try:
            conflicts = self.client.conflict_manager.get_conflicts()
            
            for conflict in conflicts:
                resolution = self._determine_resolution(conflict)
                
                if resolution:
                    self.client.conflict_manager.resolve_conflict(
                        conflict.id,
                        resolution
                    )
                    print(f"已解决冲突: {conflict.id}")
                else:
                    print(f"跳过冲突（需要人工审核）: {conflict.id}")
        except Exception as e:
            print(f"处理冲突时出错: {e}")
    
    def _determine_resolution(self, conflict):
        """根据冲突类型确定解决策略"""
        strategies = {
            ConflictType.WRITE_WRITE: ConflictResolution.KEEP_LAST,
            ConflictType.CREATE_CREATE: ConflictResolution.KEEP_ALL,
            ConflictType.WRITE_UNLINK: ConflictResolution.KEEP_FIRST,
            ConflictType.DELETE_CREATE: ConflictResolution.KEEP_LAST,
            ConflictType.RENAME_CONFLICT: None  # 需要人工审核
        }
        return strategies.get(conflict.conflict_type)

# 启动 Agent
if __name__ == "__main__":
    agent = ConflictResolutionAgent("http://localhost:9333")
    agent.run(interval=60)
```

### 示例：告警通知 Agent

```python
import time
from powerfs import PowerFSClient, EventType, NotificationLevel

class AlertNotificationAgent:
    def __init__(self, master_endpoint):
        self.client = PowerFSClient(master_endpoint=master_endpoint)
        self._setup_subscriptions()
    
    def _setup_subscriptions(self):
        """设置事件订阅"""
        self.client.notification_manager.subscribe(
            EventType.CONFLICT_DETECTED,
            self._handle_conflict
        )
        self.client.notification_manager.subscribe(
            EventType.ALERT_TRIGGERED,
            self._handle_alert
        )
        self.client.notification_manager.subscribe(
            EventType.NODE_STATUS_CHANGED,
            self._handle_node_change
        )
    
    def _handle_conflict(self, event):
        """处理冲突事件"""
        self._send_alert(
            f"检测到冲突 [{event.event_type.value}]",
            event.message,
            event.level
        )
    
    def _handle_alert(self, event):
        """处理告警事件"""
        self._send_alert(
            f"系统告警 [{event.source}]",
            event.message,
            event.level
        )
    
    def _handle_node_change(self, event):
        """处理节点状态变化"""
        status = event.payload.get('status', 'unknown')
        self._send_alert(
            f"节点状态变化 [{event.source_id}]",
            f"节点状态变为: {status}",
            event.level
        )
    
    def _send_alert(self, title, message, level):
        """发送告警通知"""
        print(f"[{level.value.upper()}] {title}")
        print(f"  {message}")
        
        # 可扩展：发送到企业微信、钉钉、邮件等
        # self._send_to_wechat(title, message, level)
        # self._send_to_dingding(title, message, level)
        # self._send_email(title, message)
    
    def start(self):
        """启动 Agent"""
        print("启动告警通知 Agent...")
        self.client.notification_manager.start_listening(poll_interval=5)
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("停止告警通知 Agent...")
            self.client.notification_manager.stop_listening()
            self.client.close()

# 启动告警 Agent
if __name__ == "__main__":
    agent = AlertNotificationAgent("http://localhost:9333")
    agent.start()
```

## 配置说明

### 环境变量

```bash
export POWERFS_MASTER_ENDPOINT=http://localhost:9333
export POWERFS_API_KEY=your-api-key
export POWERFS_TIMEOUT=30
export POWERFS_RETRIES=3
```

## 示例代码

SDK 提供了丰富的示例代码：

| 示例文件 | 描述 |
|----------|------|
| `examples/basic_usage.py` | 基础用法示例 |
| `examples/conflict_agent.py` | 自动冲突解决 Agent |
| `examples/notification_agent.py` | 告警通知 Agent |

```bash
# 运行示例
python examples/basic_usage.py
python examples/conflict_agent.py
python examples/notification_agent.py
```

## 开发

```bash
# 安装开发依赖
pip install -r requirements-dev.txt

# 运行测试
pytest tests/

# 生成文档
sphinx-build docs/ docs/_build
```

## 许可证

MIT License

## 版本历史

- **v0.1.1** (2026-07-14): 添加消息通知功能（事件订阅、Webhook 管理）
- **v0.1.0** (2026-07-14): 初始版本，支持冲突检测和解决
