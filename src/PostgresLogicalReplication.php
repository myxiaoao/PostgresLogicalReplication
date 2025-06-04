<?php

namespace Cooper\PostgresCDC;

use PDO;
use PDOException;
use Exception;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

class PostgresLogicalReplication {
    private ?PDO $conn = null;
    private string $replicationSlotName;
    private string $publicationName;
    private array $dbConfig;
    private int $lastHeartbeat;
    private int $heartbeatInterval = 10; // 心跳间隔（秒）
    private int $maxReconnectAttempts = 3; // 最大重连次数
    private int $reconnectDelay = 5; // 重连延迟（秒）
    private Logger $logger;
    private bool $isRunning = true;
    private LogicalReplicationParser $parser;
    private ?string $lastProcessedLsn = null;
    
    public function __construct(array $dbConfig, ?Logger $logger = null) {
        $this->dbConfig = $dbConfig;
        $this->replicationSlotName = $dbConfig['replication_slot_name'] ?? 'php_logical_slot';
        $this->publicationName = $dbConfig['publication_name'] ?? 'php_publication';
        $this->lastHeartbeat = time();
        $this->logger = $logger ?? $this->createDefaultLogger();
        $this->parser = new LogicalReplicationParser();
    }
    
    public function connect(): bool {
        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;application_name=%s',
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['dbname'],
            $this->dbConfig['application_name'] ?? 'php_logical_replication'
        );
        
        try {
            $this->conn = new PDO(
                $dsn,
                $this->dbConfig['user'],
                $this->dbConfig['password'],
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ]
            );
            
            return true;
        } catch (PDOException $e) {
            $this->logger->error("Connection failed: {$e->getMessage()}");
            return false;
        }
    }
    
    public function setupReplication(): bool {
        try {
            // 确保使用的是非复制连接
            $regularDsn = sprintf(
                'pgsql:host=%s;port=%s;dbname=%s;application_name=%s',
                $this->dbConfig['host'],
                $this->dbConfig['port'],
                $this->dbConfig['dbname'],
                $this->dbConfig['application_name'] ?? 'php_logical_replication'
            );
            
            $regularConn = new PDO(
                $regularDsn,
                $this->dbConfig['user'],
                $this->dbConfig['password'],
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ]
            );
            
            // 检查并设置 wal_level
            $walLevel = $regularConn->query("SHOW wal_level")->fetchColumn();
            if ($walLevel !== 'logical') {
                throw new Exception("WAL level must be set to 'logical' in postgresql.conf");
            }
            
            // 创建复制槽
            $slotExists = $regularConn->query(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName'"
            )->fetchColumn();
            
            if (!$slotExists) {
                $regularConn->exec(
                    "SELECT pg_create_logical_replication_slot('$this->replicationSlotName', 'pgoutput')"
                );
            }
            
            // 创建发布
            $publicationExists = $regularConn->query(
                "SELECT 1 FROM pg_publication WHERE pubname = '$this->publicationName'"
            )->fetchColumn();
            
            if (!$publicationExists) {
                $regularConn->exec(
                    "CREATE PUBLICATION $this->publicationName FOR ALL TABLES"
                );
            }
            
            return true;
        } catch (Exception $e) {
            $this->logger->error("Setup replication failed: {$e->getMessage()}");
            return false;
        }
    }
    
    private function sendHeartbeat(): bool {
        try {
            $this->conn?->exec("SELECT 1"); // 简单的心跳查询
            $this->lastHeartbeat = time();
            return true;
        } catch (Exception $e) {
            $this->logger->error("Heartbeat failed: {$e->getMessage()}");
            return false;
        }
    }
    
    private function checkConnection(): bool {
        if (time() - $this->lastHeartbeat > $this->heartbeatInterval) {
            return $this->sendHeartbeat();
        }
        return true;
    }
    
    private function reconnect(): bool {
        for ($attempt = 1; $attempt <= $this->maxReconnectAttempts; $attempt++) {
            $this->logger->info("Attempting to reconnect (attempt $attempt of {$this->maxReconnectAttempts})...");
            
            if ($this->connect() && $this->setupReplication()) {
                $this->logger->info("Reconnection successful");
                return true;
            }
            
            if ($attempt < $this->maxReconnectAttempts) {
                sleep($this->reconnectDelay);
            }
        }
        
        $this->logger->error("Failed to reconnect after {$this->maxReconnectAttempts} attempts");
        return false;
    }
    
    public function startReplication(callable $callback): bool {
        // 检查PHP是否安装了必要的PostgreSQL扩展
        if (!extension_loaded('pgsql')) {
            $this->logger->error("PostgreSQL extension is not loaded");
            return false;
        }
        
        // 添加一个终止标志
        $this->isRunning = true;
        
        // 注册信号处理器
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGINT, function() {
                $this->isRunning = false;
            });
            pcntl_signal(SIGTERM, function() {
                $this->isRunning = false;
            });
            if (function_exists('pcntl_async_signals')) {
                pcntl_async_signals(true);
            }
        }
        
        while ($this->isRunning) {
            try {
                if (!$this->checkConnection()) {
                    if (!$this->reconnect()) {
                        throw new Exception("Failed to maintain connection");
                    }
                }
                
                // 建立专用的复制连接，使用原生pg_connect
                $connString = sprintf(
                    "host=%s port=%s dbname=%s user=%s password=%s application_name=%s",
                    $this->dbConfig['host'],
                    $this->dbConfig['port'],
                    $this->dbConfig['dbname'],
                    $this->dbConfig['user'],
                    $this->dbConfig['password'],
                    $this->dbConfig['application_name'] ?? 'php_logical_replication'
                );
                
                $pgsqlConn = pg_connect($connString);
                if (!$pgsqlConn) {
                    throw new Exception("Failed to establish PostgreSQL connection");
                }
                
                // 检查是否存在复制槽
                $slotExists = pg_fetch_result(
                    pg_query($pgsqlConn, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName')"),
                    0, 0
                );
                
                if ($slotExists !== 't' && $slotExists !== 'true') {
                    throw new Exception("Replication slot '$this->replicationSlotName' does not exist");
                }
                
                // 使用简单的查询方式获取变更数据
                $query = "SELECT data FROM pg_logical_slot_get_binary_changes(
                    '$this->replicationSlotName', 
                    NULL, 
                    NULL,
                    'proto_version', '1',
                    'publication_names', '$this->publicationName'
                )";
                
                $this->logger->info("Starting to fetch changes from replication slot");
                
                // 主循环
                while ($this->isRunning) {
                    // 允许信号处理
                    if (function_exists('pcntl_signal_dispatch')) {
                        pcntl_signal_dispatch();
                    }
                    
                    // 获取变更数据
                    $result = pg_query($pgsqlConn, $query);
                    if (!$result) {
                        throw new Exception("Failed to get changes: " . pg_last_error($pgsqlConn));
                    }
                    
                    $hasChanges = false;
                    
                    // 处理所有变更
                    while ($row = pg_fetch_assoc($result)) {
                        $hasChanges = true;
                        if (isset($row['data'])) {
                            $this->logger->debug("Received change data");
                            
                            try {
                                // 处理二进制数据
                                $binaryData = pg_unescape_bytea($row['data']);
                                $this->logger->debug("Processing binary data");
                                
                                // 使用handleMessage方法处理消息
                                $this->handleMessage($binaryData, $callback);
                            } catch (Exception $e) {
                                $this->logger->error("Error processing change: " . $e->getMessage());
                            }
                        }
                    }
                    
                    // 释放结果
                    pg_free_result($result);
                    
                    // 在处理消息之间检查连接状态
                    if (!$this->checkConnection()) {
                        throw new Exception("Connection lost during replication");
                    }
                    
                    // 如果没有变更，等待一段时间再查询
                    if (!$hasChanges) {
                        usleep(500000); // 500ms
                    }
                }
                
                // 关闭连接
                pg_close($pgsqlConn);
                $this->logger->info("Replication stopped");
                return true;
            } catch (Exception $e) {
                $this->logger->error("Replication error: {$e->getMessage()}");
                
                if (!$this->isRunning) {
                    return false;
                }
                
                if (!$this->reconnect()) {
                    $this->logger->error("Fatal error: Unable to recover connection");
                    return false;
                }
            }
        }
        
        return true;
    }
    
    public function close(): void {
        // 设置终止标志
        $this->isRunning = false;
        
        // 关闭数据库连接
        if ($this->conn) {
            $this->conn = null;
        }
        
        $this->logger->info("Connection closed");
    }
    
    public function setHeartbeatInterval(int $seconds): void {
        $this->heartbeatInterval = $seconds;
    }
    
    public function setMaxReconnectAttempts(int $attempts): void {
        $this->maxReconnectAttempts = $attempts;
    }
    
    public function setReconnectDelay(int $seconds): void {
        $this->reconnectDelay = $seconds;
    }
    
    /**
     * 获取解析器实例
     *
     * @return LogicalReplicationParser
     */
    public function getParser(): LogicalReplicationParser {
        return $this->parser;
    }

    private function createDefaultLogger(): Logger {
        $logger = new Logger('postgres-cdc');
        $formatter = new LineFormatter(
            "[%datetime%] %channel%.%level_name%: %message% %context%\n",
            "Y-m-d H:i:s"
        );

        // CLI 输出
        $stdout = new StreamHandler('php://stdout', Logger::DEBUG);
        $stdout->setFormatter($formatter);
        $logger->pushHandler($stdout);

        // 文件日志
        $logFile = sys_get_temp_dir() . '/postgres_cdc_' . date('Y-m-d') . '.log';
        $file = new StreamHandler($logFile, Logger::DEBUG);
        $file->setFormatter($formatter);
        $logger->pushHandler($file);

        return $logger;
    }

    /**
     * 处理接收到的消息
     *
     * @param string $message 二进制消息
     * @param callable $callback 回调函数
     */
    private function handleMessage(string $message, callable $callback): void
    {
        try {
            // 检查消息是否为空
            if (empty($message)) {
                $this->logger->warning("接收到空消息");
                return;
            }
            
            // 解析消息
            $parsedData = $this->parser->parse($message);
            
            // 如果解析结果是错误类型，记录错误并返回
            if (isset($parsedData['type']) && $parsedData['type'] === 'error') {
                $this->logger->error("消息解析错误: " . ($parsedData['message'] ?? '未知错误'));
                return;
            }
            
            // 调用回调函数
            $callback($parsedData, $message);
            
            // 更新最后处理的LSN（如果存在）
            if (isset($parsedData['lsn'])) {
                $this->lastProcessedLsn = $parsedData['lsn'];
            }
        } catch (\Exception $e) {
            $this->logger->error("处理消息时发生错误: " . $e->getMessage());
            if (!empty($message)) {
                $this->logger->debug("问题消息的十六进制表示: " . bin2hex(substr($message, 0, 50)) . "...");
            }
        }
    }
}