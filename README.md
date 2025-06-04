# PostgreSQL CDC Client

基于 PostgreSQL 逻辑复制功能实现的数据变更捕获客户端，支持实时监控数据库变更，无需依赖 Kafka 等消息中间件。

## 特性

- 基于 PostgreSQL 原生逻辑复制功能
- 支持心跳检测和自动重连机制
- 实时捕获数据库变更
- 可自定义变更事件处理逻辑
- 支持优雅退出

## 安装

通过 Composer 安装：

```bash
composer require cooper/postgres-cdc
```

## 要求

- PHP >= 8.0
- PostgreSQL >= 9.4
- PDO 和 PDO_PGSQL 扩展

## PostgreSQL 配置

1. 修改 PostgreSQL 配置文件 `postgresql.conf`：

```conf
wal_level = logical         # 启用逻辑复制
max_replication_slots = 5   # 复制槽数量
max_wal_senders = 5         # 并发复制连接数
```

2. 修改 `pg_hba.conf` 允许复制连接：

```conf
host    replication     your_user    127.0.0.1/32    md5
```

3. 重启 PostgreSQL 服务以应用更改

## 基本用法

```php
use Cooper\PostgresCDC\PostgresLogicalReplication;

// 数据库配置
$dbConfig = [
    'host' => 'localhost',
    'port' => '5432',
    'dbname' => 'your_database',
    'user' => 'your_user',
    'password' => 'your_password',
    'replication_slot_name' => 'php_logical_slot',
    'publication_name' => 'php_publication'
];

// 创建实例
$replication = new PostgresLogicalReplication($dbConfig);

// 配置心跳和重连参数（可选）
$replication->setHeartbeatInterval(10);    // 心跳间隔（秒）
$replication->setMaxReconnectAttempts(3);  // 最大重试次数
$replication->setReconnectDelay(5);       // 重试间隔（秒）

// 连接数据库
if (!$replication->connect()) {
    die("Failed to connect to PostgreSQL\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("Failed to setup replication\n");
}

// 处理变更事件
$handleChange = function($change) {
    // 处理数据库变更
    echo json_encode($change, JSON_PRETTY_PRINT) . "\n";
};

// 开始监听变更
$replication->startReplication($handleChange);
```

更多示例请查看 [examples](examples) 目录。

## 长连接维护

包内置了以下机制来保持长连接的稳定性：

1. 心跳检测
   - 定期发送心跳查询检查连接状态
   - 可配置心跳间隔
   - 自动检测连接断开

2. 自动重连
   - 连接断开时自动尝试重连
   - 可配置最大重试次数和重试间隔
   - 重连成功后自动恢复复制流

3. 优雅退出
   - 支持通过信号处理优雅退出
   - 自动清理连接和资源

## 注意事项

1. 确保数据库用户具有复制权限：
```sql
ALTER ROLE your_user WITH REPLICATION;
```

2. 逻辑复制不会自动复制架构（DDL）更改，只会复制数据（DML）更改

3. 如果程序意外终止，确保正确清理复制槽：
```sql
SELECT pg_drop_replication_slot('php_logical_slot');
```

## 故障排除

1. 如果连接失败，检查：
   - PostgreSQL 配置是否正确
   - 数据库用户权限是否足够
   - 网络连接是否正常

2. 如果没有收到变更事件，检查：
   - WAL 级别是否设置为 logical
   - 复制槽是否创建成功
   - 发布是否正确创建
   - 心跳间隔是否合适

3. 如果频繁断开连接，检查：
   - 网络稳定性
   - 心跳间隔是否过长
   - 数据库负载是否过高

## License

MIT License