# PostgreSQL 逻辑复制解析器

这个 PHP 库提供了一个简单易用的方式来监听和处理 PostgreSQL 数据库的变更，使用逻辑复制（Logical Replication）功能。它使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出，并将其转换为易于使用的 PHP 数组，方便开发者实现各种 CDC（变更数据捕获）应用场景。

## 功能特点

- 自动设置 PostgreSQL 逻辑复制槽和发布
- 使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出为 JSON 格式
- 支持事务、插入、更新、删除等操作的解析
- 自动重连和错误处理
- 可定制的日志记录
- 信号处理（优雅关闭）

## 系统要求

- PHP 8.0+
- PostgreSQL 10+（启用了逻辑复制功能和安装了 wal2json 插件）
- PHP 扩展：
  - pdo
  - pdo_pgsql
  - pgsql
  - pcntl（可选，用于信号处理）
- Monolog 2.0+（用于日志记录）

## 安装

通过 Composer 安装：

```bash
composer require cooper/postgres-cdc
```

## 配置 PostgreSQL

确保 PostgreSQL 已启用逻辑复制功能，并安装了 wal2json 插件。在 `postgresql.conf` 中设置：

```
wal_level = logical
```

然后重启 PostgreSQL 服务器。

### 安装 wal2json 插件

对于大多数 PostgreSQL 发行版，wal2json 插件已经包含在 contrib 模块中。可以通过以下方式安装：

**Debian/Ubuntu:**
```bash
sudo apt-get install postgresql-contrib
```

**CentOS/RHEL:**
```bash
sudo yum install postgresql-contrib
```

如果您的 PostgreSQL 发行版没有包含 wal2json，可以从源代码编译安装：

```bash
git clone https://github.com/eulerto/wal2json.git
cd wal2json
make
make install
```

## 基本用法

```php
<?php

require_once 'vendor/autoload.php';

use Cooper\PostgresCDC\PostgresLogicalReplication;

// 数据库配置
$dbConfig = [
    'host' => 'localhost',
    'port' => '5432',
    'dbname' => 'your_database',
    'user' => 'your_username',
    'password' => 'your_password',
    'replication_slot_name' => 'my_replication_slot',
    'publication_name' => 'my_publication'
];

// 创建复制实例
$replication = new PostgresLogicalReplication($dbConfig);

// 连接数据库
if (!$replication->connect()) {
    die("无法连接到 PostgreSQL 数据库\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("无法设置复制环境\n");
}

// 定义变更处理回调函数
$handleChange = function($data, $rawJsonData = null) {
    // 根据变更类型处理数据
    if (isset($data['change'])) {
        foreach ($data['change'] as $change) {
            $kind = $change['kind'] ?? '';
            $table = $change['table'] ?? '';
            
            switch ($kind) {
                case 'insert':
                    echo "插入操作: 表 {$table}\n";
                    if (isset($change['columnvalues'])) {
                        print_r($change['columnvalues']);
                    }
                    break;
                    
                case 'update':
                    echo "更新操作: 表 {$table}\n";
                    if (isset($change['columnvalues'])) {
                        echo "新值:\n";
                        print_r($change['columnvalues']);
                    }
                    if (isset($change['oldkeys'])) {
                        echo "旧键值:\n";
                        print_r($change['oldkeys']);
                    }
                    break;
                    
                case 'delete':
                    echo "删除操作: 表 {$table}\n";
                    if (isset($change['oldkeys'])) {
                        print_r($change['oldkeys']);
                    }
                    break;
            }
        }
    }
};

// 开始监听变更
try {
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "错误: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}
```

## 配置选项

### 数据库配置

| 参数 | 描述 | 默认值 |
|------|------|--------|
| host | PostgreSQL 服务器主机 | - |
| port | PostgreSQL 服务器端口 | - |
| dbname | 数据库名称 | - |
| user | 用户名 | - |
| password | 密码 | - |
| replication_slot_name | 复制槽名称 | php_logical_slot |
| publication_name | 发布名称 | php_publication |
| application_name | 应用名称 | php_logical_replication |

### 方法

| 方法 | 描述 |
|------|------|
| connect() | 连接到 PostgreSQL 数据库 |
| setupReplication() | 设置复制环境（创建复制槽和发布） |
| startReplication(callable $callback) | 开始监听变更数据 |
| close() | 关闭连接 |
| setHeartbeatInterval(int $seconds) | 设置心跳间隔（秒） |
| setMaxReconnectAttempts(int $attempts) | 设置最大重连次数 |
| setReconnectDelay(int $seconds) | 设置重连延迟（秒） |
| recreateReplicationSlot() | 重新创建复制槽 |

## wal2json 输出格式

wal2json 插件输出的 JSON 数据格式如下：

### 插入操作

```json
{
  "change": [
    {
      "kind": "insert",
      "schema": "public",
      "table": "users",
      "columnnames": ["id", "name", "email"],
      "columntypes": ["integer", "character varying(255)", "character varying(255)"],
      "columnvalues": [1, "张三", "zhangsan@example.com"]
    }
  ]
}
```

### 更新操作

```json
{
  "change": [
    {
      "kind": "update",
      "schema": "public",
      "table": "users",
      "columnnames": ["id", "name", "email"],
      "columntypes": ["integer", "character varying(255)", "character varying(255)"],
      "columnvalues": [1, "张三", "zhangsan_new@example.com"],
      "oldkeys": {
        "keynames": ["id"],
        "keytypes": ["integer"],
        "keyvalues": [1]
      }
    }
  ]
}
```

### 删除操作

```json
{
  "change": [
    {
      "kind": "delete",
      "schema": "public",
      "table": "users",
      "oldkeys": {
        "keynames": ["id"],
        "keytypes": ["integer"],
        "keyvalues": [1]
      }
    }
  ]
}
```

## 关于 wal2json 插件

wal2json 是 PostgreSQL 的一个逻辑解码输出插件，它将 WAL（预写式日志）中的变更转换为 JSON 格式。这使得处理和消费这些变更变得更加容易，特别是对于需要与其他系统集成的应用程序。

wal2json 支持以下功能：

- 将 INSERT、UPDATE、DELETE 操作转换为 JSON 格式
- 支持事务边界（开始和提交）
- 提供列名、类型和值
- 提供主键信息
- 支持多种配置选项，如时间戳、模式名称等

更多关于 wal2json 的信息，请访问 [wal2json GitHub 仓库](https://github.com/eulerto/wal2json)。

## 许可证

MIT
