<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Cooper\PostgreCDC\PostgreLogicalReplication;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

// 数据库配置
$dbConfig = [
    'host' => 'localhost',
    'port' => '5432',
    'dbname' => 'test',
    'user' => 'postgres',
    'password' => 'postgres',
    'replication_slot_name' => 'php_logical_slot',
    'publication_name' => 'php_publication',
    'application_name' => 'php_logical_replication'
];

// 创建自定义日志实例
$logger = new Logger('POSTGRE-CDC');
$formatter = new LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message%\n",
    "Y-m-d H:i:s"
);

// 添加控制台输出
$stdout = new StreamHandler('php://stdout', Logger::DEBUG);
$stdout->setFormatter($formatter);
$logger->pushHandler($stdout);

// 添加文件日志
$logDir = __DIR__ . '/../logs';
if (!is_dir($logDir) && !mkdir($logDir, 0755, true) && !is_dir($logDir)) {
    throw new \RuntimeException(sprintf('目录 "%s" 无法创建', $logDir));
}
$logFile = $logDir . '/postgre_cdc.log';
$file = new StreamHandler($logFile, Logger::DEBUG);
$file->setFormatter($formatter);
$logger->pushHandler($file);

// 创建复制实例，注入日志实例
$replication = new PostgreLogicalReplication($dbConfig, $logger);

// 配置心跳和重连参数
$replication->setHeartbeatInterval(10);  // 10 秒发送一次心跳
$replication->setMaxReconnectAttempts(3); // 最多重试 3 次
$replication->setReconnectDelay(5);      // 重试间隔 5 秒

// 连接数据库
if (!$replication->connect()) {
    die("无法连接到 PostgreSQL 数据库\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("无法设置复制环境\n");
}

// 定义变更处理回调函数
$handleChange = function ($data, $rawJsonData = null) {
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

                default:
                    echo "其他操作: {$kind}\n";
                    print_r($change);
            }

            echo "\n";
        }
    } else {
        // 处理其他类型的消息
        print_r($data);
    }

    echo "\n";
};

// 开始监听变更
try {
    echo "开始监听 PostgreSQL 数据变更...\n";
    echo "按 Ctrl+C 停止\n\n";

    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "错误: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}
