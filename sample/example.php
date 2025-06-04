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
    'dbname' => 'test',
    'user' => 'postgres',
    'password' => 'postgres',
    'replication_slot_name' => 'php_logical_slot',
    'publication_name' => 'php_publication',
    'application_name' => 'php_logical_replication'
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
$handleChange = function($parsedData) {
    echo "接收到 PostgreSQL 变更数据:\n";
    
    // 根据消息类型处理内容
    switch($parsedData['type']) {
        case 'begin':
            echo "开始事务:\n";
            if (isset($parsedData['lsn'])) {
                echo "  LSN: {$parsedData['lsn']}\n";
            }
            if (isset($parsedData['timestamp_formatted'])) {
                echo "  时间戳: {$parsedData['timestamp_formatted']}\n";
            }
            if (isset($parsedData['xid'])) {
                echo "  事务ID: {$parsedData['xid']}\n";
            }
            break;
            
        case 'commit':
            echo "提交事务:\n";
            if (isset($parsedData['flags'])) {
                echo "  标志: {$parsedData['flags']}\n";
            }
            if (isset($parsedData['lsn'])) {
                echo "  LSN: {$parsedData['lsn']}\n";
            }
            if (isset($parsedData['end_lsn'])) {
                echo "  结束LSN: {$parsedData['end_lsn']}\n";
            }
            if (isset($parsedData['timestamp_formatted'])) {
                echo "  时间戳: {$parsedData['timestamp_formatted']}\n";
            }
            break;
            
        case 'relation':
            echo "关系定义:\n";
            
            // 检查必要的键是否存在
            if (isset($parsedData['relation_id'])) {
                echo "  关系ID: {$parsedData['relation_id']}\n";
            }
            
            if (isset($parsedData['namespace'])) {
                echo "  命名空间: {$parsedData['namespace']}\n";
            }
            
            if (isset($parsedData['relation_name'])) {
                echo "  表名: {$parsedData['relation_name']}\n";
            }
            
            if (isset($parsedData['replica_identity'])) {
                echo "  复制标识: {$parsedData['replica_identity']}\n";
            }
            
            // 输出列信息
            if (isset($parsedData['columns']) && is_array($parsedData['columns'])) {
                echo "  列数量: " . count($parsedData['columns']) . "\n";
                echo "  列信息:\n";
                foreach ($parsedData['columns'] as $column) {
                    $keyFlag = isset($column['is_key']) && $column['is_key'] ? "[主键]" : "";
                    $typeId = $column['data_type_id'] ?? '未知';
                    $name = $column['name'] ?? '未知';
                    echo "    - {$name} (类型ID: {$typeId}) $keyFlag\n";
                }
            }
            break;
            
        case 'insert':
            echo "插入操作:\n";
            echo "  表: " . ($parsedData['table'] ?? "未知表") . "\n";
            
            // 检查 relation_id 是否存在
            if (isset($parsedData['relation_id'])) {
                echo "  关系ID: {$parsedData['relation_id']}\n";
            }
            
            // 输出主键信息
            if (!empty($parsedData['primary_keys'])) {
                echo "  主键列: " . implode(', ', $parsedData['primary_keys']) . "\n";
            }
            
            // 输出数据
            echo "  数据:\n";
            if (empty($parsedData['data'])) {
                echo "    没有数据或数据解析失败\n";
            } else {
                foreach ($parsedData['data'] as $key => $value) {
                    if ($value === null) {
                        $valueStr = "NULL";
                    } elseif (is_array($value)) {
                        $valueStr = json_encode($value, JSON_UNESCAPED_UNICODE);
                    } elseif (is_bool($value)) {
                        $valueStr = $value ? "true" : "false";
                    } else {
                        $valueStr = (string)$value;
                    }
                    echo "    - $key: $valueStr\n";
                }
            }
            break;
            
        case 'update':
            echo "更新操作:\n";
            echo "  表: " . ($parsedData['table'] ?? "未知表") . "\n";
            
            // 检查 relation_id 是否存在
            if (isset($parsedData['relation_id'])) {
                echo "  关系ID: {$parsedData['relation_id']}\n";
            }
            
            // 输出主键信息
            if (!empty($parsedData['primary_keys'])) {
                echo "  主键列: " . implode(', ', $parsedData['primary_keys']) . "\n";
            }
            
            // 输出旧数据（如果有）
            if (isset($parsedData['old_data']) && $parsedData['old_data'] !== null) {
                echo "  旧数据:\n";
                if (empty($parsedData['old_data'])) {
                    echo "    没有旧数据或数据解析失败\n";
                } else {
                    foreach ($parsedData['old_data'] as $key => $value) {
                        if ($value === null) {
                            $valueStr = "NULL";
                        } elseif (is_array($value)) {
                            $valueStr = json_encode($value, JSON_UNESCAPED_UNICODE);
                        } elseif (is_bool($value)) {
                            $valueStr = $value ? "true" : "false";
                        } else {
                            $valueStr = (string)$value;
                        }
                        echo "    - $key: $valueStr\n";
                    }
                }
            }
            
            // 输出新数据
            echo "  新数据:\n";
            if (empty($parsedData['new_data'])) {
                echo "    没有新数据或数据解析失败\n";
            } else {
                foreach ($parsedData['new_data'] as $key => $value) {
                    if ($value === null) {
                        $valueStr = "NULL";
                    } elseif (is_array($value)) {
                        $valueStr = json_encode($value, JSON_UNESCAPED_UNICODE);
                    } elseif (is_bool($value)) {
                        $valueStr = $value ? "true" : "false";
                    } else {
                        $valueStr = (string)$value;
                    }
                    echo "    - $key: $valueStr\n";
                }
            }
            break;
            
        case 'delete':
            echo "删除操作:\n";
            echo "  表: " . ($parsedData['table'] ?? "未知表") . "\n";
            
            // 检查 relation_id 是否存在
            if (isset($parsedData['relation_id'])) {
                echo "  关系ID: {$parsedData['relation_id']}\n";
            }
            
            // 输出主键信息
            if (!empty($parsedData['primary_keys'])) {
                echo "  主键列: " . implode(', ', $parsedData['primary_keys']) . "\n";
            }
            
            // 输出数据
            echo "  数据:\n";
            if (empty($parsedData['data'])) {
                echo "    没有数据或数据解析失败\n";
            } else {
                foreach ($parsedData['data'] as $key => $value) {
                    if ($value === null) {
                        $valueStr = "NULL";
                    } elseif (is_array($value)) {
                        $valueStr = json_encode($value, JSON_UNESCAPED_UNICODE);
                    } elseif (is_bool($value)) {
                        $valueStr = $value ? "true" : "false";
                    } else {
                        $valueStr = (string)$value;
                    }
                    echo "    - $key: $valueStr\n";
                }
            }
            break;
            
        case 'truncate':
            echo "截断操作:\n";
            echo "  表: " . ($parsedData['table'] ?? "未知表") . "\n";
            break;
            
        case 'message':
            echo "逻辑复制消息:\n";
            echo "  内容: " . ($parsedData['content'] ?? "无内容") . "\n";
            echo "  前缀: " . ($parsedData['prefix'] ?? "无前缀") . "\n";
            echo "  事务性: " . (($parsedData['transactional'] ?? false) ? "是" : "否") . "\n";
            break;
            
        default:
            echo "未知操作类型: " . $parsedData['type'] . "\n";
            print_r($parsedData);
    }
    
    echo "\n";
};

// 开始监听变更
try {
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}