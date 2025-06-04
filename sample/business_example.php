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

// 创建日志实例
$logger = new Monolog\Logger('business');
$formatter = new Monolog\Formatter\LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message%\n",
    "Y-m-d H:i:s"
);

$stdout = new Monolog\Handler\StreamHandler('php://stdout', Monolog\Logger::DEBUG);
$stdout->setFormatter($formatter);
$logger->pushHandler($stdout);

// 创建复制实例
$replication = new PostgresLogicalReplication($dbConfig, $logger);

// 连接数据库
if (!$replication->connect()) {
    die("无法连接到PostgreSQL数据库\n");
}

// 设置复制
if (!$replication->setupReplication()) {
    die("无法设置逻辑复制\n");
}

// 模拟缓存系统
$cache = [];

// 模拟消息队列
$messageQueue = [];

// 定义变更处理回调函数
$handleChange = function($parsedData) use (&$cache, &$messageQueue, $logger) {
    // 只处理数据操作消息（插入、更新、删除）
    if (!in_array($parsedData['type'], ['insert', 'update', 'delete'])) {
        return;
    }
    
    // 获取表名
    $table = $parsedData['table'] ?? null;
    if (!$table) {
        $logger->warning("收到没有表名的数据操作消息");
        return;
    }
    
    $logger->info("处理表 {$table} 的 {$parsedData['type']} 操作");
    
    // 根据表名和操作类型执行不同的业务逻辑
    switch ($table) {
        case 'public.users':
            handleUserChanges($parsedData, $cache, $messageQueue, $logger);
            break;
            
        case 'public.products':
            handleProductChanges($parsedData, $cache, $messageQueue, $logger);
            break;
            
        case 'public.orders':
            handleOrderChanges($parsedData, $cache, $messageQueue, $logger);
            break;
            
        default:
            $logger->info("没有为表 {$table} 定义特定的处理逻辑");
    }
    
    // 处理消息队列（在实际应用中，这里会将消息发送到实际的消息队列）
    processMessageQueue($messageQueue, $logger);
};

// 开始监听变更
try {
    $logger->info("开始监控数据库变更...");
    $logger->info("按 Ctrl+C 退出");
    $replication->startReplication($handleChange);
} catch (Exception $e) {
    $logger->error("错误: " . $e->getMessage());
} finally {
    $replication->close();
}

/**
 * 处理用户表的变更
 */
function handleUserChanges(array $data, array &$cache, array &$messageQueue, Logger $logger): void {
    switch ($data['type']) {
        case 'insert':
            $userData = $data['data'];
            $userId = $userData['id'] ?? null;
            
            if ($userId) {
                // 更新缓存
                $cache["user:{$userId}"] = $userData;
                $logger->info("用户 {$userId} 已添加到缓存");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'user_created',
                    'payload' => $userData
                ];
            }
            break;
            
        case 'update':
            $userData = $data['new_data'];
            $userId = $userData['id'] ?? null;
            
            if ($userId) {
                // 更新缓存
                $cache["user:{$userId}"] = $userData;
                $logger->info("用户 {$userId} 缓存已更新");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'user_updated',
                    'payload' => [
                        'old' => $data['old_data'] ?? [],
                        'new' => $userData
                    ]
                ];
            }
            break;
            
        case 'delete':
            $userData = $data['data'];
            $userId = $userData['id'] ?? null;
            
            if ($userId) {
                // 从缓存中删除
                unset($cache["user:{$userId}"]);
                $logger->info("用户 {$userId} 已从缓存中删除");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'user_deleted',
                    'payload' => $userData
                ];
            }
            break;
    }
}

/**
 * 处理产品表的变更
 */
function handleProductChanges(array $data, array &$cache, array &$messageQueue, Logger $logger): void {
    switch ($data['type']) {
        case 'insert':
            $productData = $data['data'];
            $productId = $productData['id'] ?? null;
            
            if ($productId) {
                // 更新缓存
                $cache["product:{$productId}"] = $productData;
                $logger->info("产品 {$productId} 已添加到缓存");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'product_created',
                    'payload' => $productData
                ];
            }
            break;
            
        case 'update':
            $productData = $data['new_data'];
            $productId = $productData['id'] ?? null;
            
            if ($productId) {
                // 检查库存变化
                $oldStock = $data['old_data']['stock'] ?? null;
                $newStock = $productData['stock'] ?? null;
                
                if ($oldStock !== null && $newStock !== null && $oldStock != $newStock) {
                    $logger->info("产品 {$productId} 库存从 {$oldStock} 变更为 {$newStock}");
                    
                    // 库存变化通知
                    $messageQueue[] = [
                        'topic' => 'product_stock_changed',
                        'payload' => [
                            'product_id' => $productId,
                            'old_stock' => $oldStock,
                            'new_stock' => $newStock
                        ]
                    ];
                }
                
                // 更新缓存
                $cache["product:{$productId}"] = $productData;
                $logger->info("产品 {$productId} 缓存已更新");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'product_updated',
                    'payload' => [
                        'old' => $data['old_data'] ?? [],
                        'new' => $productData
                    ]
                ];
            }
            break;
            
        case 'delete':
            $productData = $data['data'];
            $productId = $productData['id'] ?? null;
            
            if ($productId) {
                // 从缓存中删除
                unset($cache["product:{$productId}"]);
                $logger->info("产品 {$productId} 已从缓存中删除");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'product_deleted',
                    'payload' => $productData
                ];
            }
            break;
    }
}

/**
 * 处理订单表的变更
 */
function handleOrderChanges(array $data, array &$cache, array &$messageQueue, Logger $logger): void {
    switch ($data['type']) {
        case 'insert':
            $orderData = $data['data'];
            $orderId = $orderData['id'] ?? null;
            
            if ($orderId) {
                // 更新缓存
                $cache["order:{$orderId}"] = $orderData;
                $logger->info("订单 {$orderId} 已添加到缓存");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'order_created',
                    'payload' => $orderData
                ];
            }
            break;
            
        case 'update':
            $orderData = $data['new_data'];
            $orderId = $orderData['id'] ?? null;
            
            if ($orderId) {
                // 检查订单状态变化
                $oldStatus = $data['old_data']['status'] ?? null;
                $newStatus = $orderData['status'] ?? null;
                
                if ($oldStatus !== null && $newStatus !== null && $oldStatus != $newStatus) {
                    $logger->info("订单 {$orderId} 状态从 {$oldStatus} 变更为 {$newStatus}");
                    
                    // 订单状态变化通知
                    $messageQueue[] = [
                        'topic' => 'order_status_changed',
                        'payload' => [
                            'order_id' => $orderId,
                            'old_status' => $oldStatus,
                            'new_status' => $newStatus
                        ]
                    ];
                }
                
                // 更新缓存
                $cache["order:{$orderId}"] = $orderData;
                $logger->info("订单 {$orderId} 缓存已更新");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'order_updated',
                    'payload' => [
                        'old' => $data['old_data'] ?? [],
                        'new' => $orderData
                    ]
                ];
            }
            break;
            
        case 'delete':
            $orderData = $data['data'];
            $orderId = $orderData['id'] ?? null;
            
            if ($orderId) {
                // 从缓存中删除
                unset($cache["order:{$orderId}"]);
                $logger->info("订单 {$orderId} 已从缓存中删除");
                
                // 添加到消息队列
                $messageQueue[] = [
                    'topic' => 'order_deleted',
                    'payload' => $orderData
                ];
            }
            break;
    }
}

/**
 * 处理消息队列
 */
function processMessageQueue(array &$messageQueue, Logger $logger): void {
    if (empty($messageQueue)) {
        return;
    }
    
    $logger->info("处理消息队列，共 " . count($messageQueue) . " 条消息");
    
    foreach ($messageQueue as $message) {
        $topic = $message['topic'];
        $payload = $message['payload'];
        
        // 在实际应用中，这里会将消息发送到实际的消息队列系统（如RabbitMQ、Kafka等）
        $logger->info("发送消息到主题 '{$topic}': " . json_encode($payload, JSON_UNESCAPED_UNICODE));
    }
    
    // 清空消息队列
    $messageQueue = [];
} 