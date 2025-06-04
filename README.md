# PostgreSQL 逻辑复制解析器

这个 PHP 库提供了一个简单易用的方式来监听和处理 PostgreSQL 数据库的变更，使用逻辑复制（Logical Replication）功能。它使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出，并将其转换为易于使用的 PHP 数组，方便开发者实现各种 CDC（变更数据捕获）应用场景。

## 功能特点

- 自动设置 PostgreSQL 逻辑复制槽和发布
- 使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出为 JSON 格式
- 将表结构信息与数据变更关联
- 支持事务、插入、更新、删除等操作的解析
- 提供友好的 PHP 数组格式的变更数据
- 自动重连和错误处理
- 可定制的日志记录

## 系统要求

- PHP 8.0+
- PostgreSQL 10+（启用了逻辑复制功能和安装了 wal2json 插件）
- PHP PostgreSQL 扩展（pgsql）
- Monolog 库（用于日志记录）

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
    die("Failed to connect to PostgreSQL\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("Failed to setup replication\n");
}

// 定义变更处理回调函数
$handleChange = function($parsedData) {
    // 根据消息类型处理内容
    switch($parsedData['type']) {
        case 'insert':
            echo "插入操作: 表 {$parsedData['table']}\n";
            print_r($parsedData['data']);
            break;
            
        case 'update':
            echo "更新操作: 表 {$parsedData['table']}\n";
            echo "旧数据:\n";
            print_r($parsedData['old_data'] ?? []);
            echo "新数据:\n";
            print_r($parsedData['new_data']);
            break;
            
        case 'delete':
            echo "删除操作: 表 {$parsedData['table']}\n";
            print_r($parsedData['data']);
            break;
            
        case 'message':
            echo "逻辑复制消息:\n";
            echo "内容: {$parsedData['content']}\n";
            echo "前缀: {$parsedData['prefix']}\n";
            echo "事务性: " . ($parsedData['transactional'] ? 'true' : 'false') . "\n";
            break;
    }
};

// 开始监听变更
try {
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}
```

## 高级用法

查看 `sample/business_example.php` 文件，了解如何使用解析后的数据实现业务逻辑，例如：

- 更新缓存
- 发送消息到消息队列
- 触发特定业务事件
- 处理不同表的变更

## 解析后的数据格式

根据消息类型，解析后的数据有不同的格式：

### 事务开始（begin）

```php
[
    'type' => 'begin',
    'lsn' => 123456789,
    'timestamp' => 1621234567,
    'timestamp_formatted' => '2021-05-17 12:34:56',
    'xid' => 12345
]
```

### 事务提交（commit）

```php
[
    'type' => 'commit',
    'flags' => 0,
    'lsn' => 123456789,
    'end_lsn' => 123456790,
    'timestamp' => 1621234567,
    'timestamp_formatted' => '2021-05-17 12:34:56'
]
```

### 插入操作（insert）

```php
[
    'type' => 'insert',
    'relation_id' => 12345,
    'table' => 'public.users',
    'data' => [
        'id' => '1',
        'name' => '张三',
        'email' => 'zhangsan@example.com',
        'created_at' => '2021-05-17 12:34:56'
    ],
    'primary_keys' => ['id']
]
```

### 更新操作（update）

```php
[
    'type' => 'update',
    'relation_id' => 12345,
    'table' => 'public.users',
    'has_old_tuple' => true,
    'old_data' => [
        'id' => '1',
        'name' => '张三',
        'email' => 'zhangsan@example.com',
        'updated_at' => '2021-05-17 12:34:56'
    ],
    'new_data' => [
        'id' => '1',
        'name' => '张三',
        'email' => 'zhangsan_new@example.com',
        'updated_at' => '2021-05-17 12:45:00'
    ],
    'primary_keys' => ['id']
]
```

### 删除操作（delete）

```php
[
    'type' => 'delete',
    'relation_id' => 12345,
    'table' => 'public.users',
    'data' => [
        'id' => '1',
        'name' => '张三',
        'email' => 'zhangsan@example.com'
    ],
    'primary_keys' => ['id']
]
```

### 消息操作（message）

```php
[
    'type' => 'message',
    'transactional' => true,
    'prefix' => 'my_app',
    'content' => '这是一条通过 pg_logical_emit_message 发送的消息'
]
```

## 关于 wal2json 插件

wal2json 是 PostgreSQL 的一个逻辑解码输出插件，它将 WAL（预写式日志）中的变更转换为 JSON 格式。这使得处理和消费这些变更变得更加容易，特别是对于需要与其他系统集成的应用程序。

wal2json 支持以下功能：

- 将 INSERT、UPDATE、DELETE 操作转换为 JSON 格式
- 支持事务边界（开始和提交）
- 支持逻辑复制消息
- 提供列名、类型和值
- 提供主键信息
- 支持多种配置选项，如时间戳、模式名称等

更多关于 wal2json 的信息，请访问 [wal2json GitHub 仓库](https://github.com/eulerto/wal2json)。

## 许可证

MIT
