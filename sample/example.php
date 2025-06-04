<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Cooper\PostgresCDC\PostgresLogicalReplication;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

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

// 创建自定义日志实例（可选）
$logger = new Monolog\Logger('custom');
$formatter = new Monolog\Formatter\LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message%\n",
    "Y-m-d H:i:s"
);

// 添加控制台输出
$stdout = new Monolog\Handler\StreamHandler('php://stdout', Monolog\Logger::DEBUG);
$stdout->setFormatter($formatter);
$logger->pushHandler($stdout);

// 添加文件日志
$logDir = __DIR__ . '/../logs';
if (!is_dir($logDir)) {
    mkdir($logDir, 0755, true);
}
$logFile = $logDir . '/postgres_cdc.log';
$file = new Monolog\Handler\StreamHandler($logFile, Monolog\Logger::DEBUG);
$file->setFormatter($formatter);
$logger->pushHandler($file);

// 创建复制实例，注入日志实例
$replication = new PostgresLogicalReplication($dbConfig, $logger);

// 或使用默认日志配置
// $replication = new PostgresLogicalReplication($dbConfig);

// 配置心跳和重连参数
$replication->setHeartbeatInterval(10);  // 10 秒发送一次心跳
$replication->setMaxReconnectAttempts(3); // 最多重试 3 次
$replication->setReconnectDelay(5);      // 重试间隔 5 秒

// 连接数据库
if (!$replication->connect()) {
    die("Failed to connect to PostgreSQL\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("Failed to setup replication\n");
}

// 定义变更处理回调函数
$handleChange = function($change) {
    echo "Received database change:\n";
    echo json_encode($change, JSON_PRETTY_PRINT) . "\n";
    
    // 这里可以添加自定义的处理逻辑
    // 例如：发送通知、更新缓存、触发其他操作等
};

// 注册信号处理器，用于优雅退出
declare(ticks = 1);
pcntl_signal(SIGTERM, function() use ($replication) {
    echo "Received SIGTERM signal, closing connection...\n";
    $replication->close();
    exit(0);
});

pcntl_signal(SIGINT, function() use ($replication) {
    echo "Received SIGINT signal, closing connection...\n";
    $replication->close();
    exit(0);
});

// 开始监听变更
try {
    echo "Starting to monitor database changes...\n";
    echo "Press Ctrl+C to exit\n";
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}