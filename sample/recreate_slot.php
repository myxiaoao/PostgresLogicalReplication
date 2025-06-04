<?php

/**
 * 这个脚本用于重新创建复制槽
 * 当需要从 pgoutput 更改为 wal2json 插件时使用
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Cooper\PostgresCDC\PostgresLogicalReplication;

// 数据库配置
$dbConfig = [
    'host' => 'localhost',
    'port' => '5432',
    'dbname' => 'postgres',
    'user' => 'postgres',
    'password' => 'postgres',
    'replication_slot_name' => 'php_logical_slot',
    'publication_name' => 'php_publication',
    'application_name' => 'php_logical_replication'
];

// 创建复制实例
$replication = new PostgresLogicalReplication($dbConfig);

// 连接数据库
if (!$replication->connect()) {
    die("Failed to connect to PostgreSQL\n");
}

echo "正在重新创建复制槽 {$dbConfig['replication_slot_name']} 使用 wal2json 插件...\n";

// 强制重新创建复制槽
if (!$replication->recreateReplicationSlot()) {
    die("Failed to recreate replication slot\n");
}

echo "复制槽已成功重新创建！\n";

// 关闭连接
$replication->close(); 