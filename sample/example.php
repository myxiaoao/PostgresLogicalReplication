<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Cooper\PostgresCDC\PostgresLogicalReplication;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

// 调试选项
$debug = [
    'show_binary_data' => false,  // 设置为true以显示原始二进制数据
    'show_hex_dump' => false      // 设置为true以显示十六进制转储
];

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
$handleChange = function($parsedData, $binaryData = null) use ($debug) {
    echo "接收到PostgreSQL变更数据:\n";
    
    // 显示调试信息（如果启用）
    if ($debug['show_binary_data'] && $binaryData !== null) {
        echo "原始二进制数据长度: " . strlen($binaryData) . " 字节\n";
        
        if ($debug['show_hex_dump']) {
            echo "十六进制转储:\n";
            echo hexDump($binaryData) . "\n";
        }
    }
    
    // 根据消息类型处理内容
    switch($parsedData['type']) {
        case 'begin':
            echo "开始事务:\n";
            echo "  LSN: {$parsedData['lsn']}\n";
            echo "  时间戳: {$parsedData['timestamp_formatted']}\n";
            echo "  事务ID: {$parsedData['xid']}\n";
            break;
            
        case 'commit':
            echo "提交事务:\n";
            echo "  标志: {$parsedData['flags']}\n";
            echo "  LSN: {$parsedData['lsn']}\n";
            echo "  结束LSN: {$parsedData['end_lsn']}\n";
            echo "  时间戳: {$parsedData['timestamp_formatted']}\n";
            break;
            
        case 'relation':
            echo "关系定义:\n";
            echo "  关系ID: {$parsedData['relation_id']}\n";
            echo "  命名空间: {$parsedData['namespace']}\n";
            echo "  表名: {$parsedData['relation_name']}\n";
            echo "  复制标识: {$parsedData['replica_identity']}\n";
            echo "  列数量: " . count($parsedData['columns']) . "\n";
            
            // 输出列信息
            echo "  列信息:\n";
            foreach ($parsedData['columns'] as $column) {
                $keyFlag = $column['is_key'] ? "[主键]" : "";
                echo "    - {$column['name']} (类型ID: {$column['data_type_id']}) $keyFlag\n";
            }
            break;
            
        case 'insert':
            echo "插入操作:\n";
            echo "  表: " . ($parsedData['table'] ?? "未知表") . "\n";
            echo "  关系ID: {$parsedData['relation_id']}\n";
            
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
            echo "  关系ID: {$parsedData['relation_id']}\n";
            
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
            echo "  关系ID: {$parsedData['relation_id']}\n";
            
            // 输出主键信息
            if (!empty($parsedData['primary_keys'])) {
                echo "  主键列: " . implode(', ', $parsedData['primary_keys']) . "\n";
            }
            
            // 检查是否有映射错误
            if (isset($parsedData['mapping_error'])) {
                echo "  映射错误: {$parsedData['mapping_error']}\n";
            }
            
            // 输出数据
            echo "  数据:\n";
            if (empty($parsedData['data'])) {
                echo "    没有数据或数据解析失败\n";
                
                // 显示原始二进制数据的十六进制表示（如果可用）
                if ($binaryData !== null) {
                    echo "  原始数据(十六进制): " . bin2hex(substr($binaryData, 0, 50)) . "...\n";
                }
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
            echo "截断表操作:\n";
            echo "  级联: " . ($parsedData['cascade'] ? "是" : "否") . "\n";
            echo "  重置自增ID: " . ($parsedData['restart_identity'] ? "是" : "否") . "\n";
            echo "  表数量: " . count($parsedData['relations']) . "\n";
            break;
            
        default:
            echo "未知或未处理的消息类型: {$parsedData['type']}\n";
            echo "原始数据: " . json_encode($parsedData, JSON_UNESCAPED_UNICODE) . "\n";
    }
    
    echo "----------------------------------------\n";
    
    // 这里可以添加自定义的业务逻辑
    // 例如：根据变更数据更新缓存、发送通知、触发其他操作等
};

// 开始监听变更
try {
    echo "开始监控数据库变更...\n";
    echo "按 Ctrl+C 退出\n";
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    echo "错误: " . $e->getMessage() . "\n";
} finally {
    $replication->close();
}

/**
 * 生成二进制数据的十六进制转储
 *
 * @param string $data 二进制数据
 * @param int $bytesPerLine 每行显示的字节数
 * @return string 格式化的十六进制转储
 */
function hexDump($data, $bytesPerLine = 16) {
    $hexDump = '';
    $hexChars = '';
    $asciiChars = '';
    $length = strlen($data);
    
    for ($i = 0; $i < $length; $i++) {
        // 新行开始
        if ($i % $bytesPerLine === 0) {
            if ($i > 0) {
                $hexDump .= sprintf("  %s\n", $asciiChars);
                $asciiChars = '';
            }
            $hexDump .= sprintf("%08X: ", $i);
        }
        
        // 获取当前字节的十六进制和ASCII表示
        $byte = ord($data[$i]);
        $hexChars = sprintf("%02X ", $byte);
        $asciiChar = ($byte >= 32 && $byte <= 126) ? $data[$i] : '.';
        
        $hexDump .= $hexChars;
        $asciiChars .= $asciiChar;
        
        // 每8个字节添加一个额外的空格
        if (($i + 1) % 8 === 0 && ($i + 1) % $bytesPerLine !== 0) {
            $hexDump .= ' ';
        }
    }
    
    // 补齐最后一行
    $remaining = $length % $bytesPerLine;
    if ($remaining > 0) {
        $padding = $bytesPerLine - $remaining;
        $hexDump .= str_repeat('   ', $padding);
        
        // 添加额外的空格（如果需要）
        if ($remaining <= 8) {
            $hexDump .= ' ';
        }
    }
    
    // 添加最后一行的ASCII部分
    $hexDump .= sprintf("  %s", $asciiChars);
    
    return $hexDump;
}