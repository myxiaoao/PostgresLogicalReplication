# PostgreSQL逻辑复制解析器

这个PHP库提供了一个简单易用的方式来监听和处理PostgreSQL数据库的变更，使用逻辑复制（Logical Replication）功能。它能够解析PostgreSQL的二进制输出格式，并将其转换为易于使用的PHP数组，方便开发者实现各种CDC（变更数据捕获）应用场景。

## 功能特点

- 自动设置PostgreSQL逻辑复制槽和发布
- 解析PostgreSQL的二进制逻辑复制格式（pgoutput）
- 将表结构信息与数据变更关联
- 支持事务、插入、更新、删除等操作的解析
- 提供友好的PHP数组格式的变更数据
- 自动重连和错误处理
- 可定制的日志记录

## 系统要求

- PHP 8.0+
- PostgreSQL 10+（启用了逻辑复制功能）
- PHP PostgreSQL扩展（pgsql）
- Monolog库（用于日志记录）

## 安装

通过Composer安装：

```bash
composer require cooper/postgres-cdc
```

## 配置PostgreSQL

确保PostgreSQL已启用逻辑复制功能。在`postgresql.conf`中设置：

```
wal_level = logical
```

然后重启PostgreSQL服务器。

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

## 许可证

MIT
