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
            
            // 检查复制槽是否存在
            $slotInfo = $regularConn->query(
                "SELECT slot_name, plugin FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName'"
            )->fetch(PDO::FETCH_ASSOC);
            
            // 如果复制槽存在但不是使用 wal2json 插件，则删除它
            if ($slotInfo && $slotInfo['plugin'] !== 'wal2json') {
                $this->logger->info("删除现有复制槽 {$this->replicationSlotName}，因为它使用的是 {$slotInfo['plugin']} 插件而不是 wal2json");
                $regularConn->exec(
                    "SELECT pg_drop_replication_slot('$this->replicationSlotName')"
                );
                $slotInfo = null;
            }
            
            // 创建复制槽（如果不存在或已被删除）
            if (!$slotInfo) {
                $this->logger->info("创建新的复制槽 {$this->replicationSlotName} 使用 wal2json 插件");
                $regularConn->exec(
                    "SELECT pg_create_logical_replication_slot('$this->replicationSlotName', 'wal2json')"
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
                
                // 检查复制槽是否使用 wal2json 插件
                $pluginName = pg_fetch_result(
                    pg_query($pgsqlConn, "SELECT plugin FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName'"),
                    0, 0
                );
                
                if ($pluginName !== 'wal2json') {
                    throw new Exception("Replication slot '$this->replicationSlotName' is using plugin '$pluginName' instead of 'wal2json'");
                }
                
                // 使用 wal2json 插件获取 JSON 格式的变更数据
                $query = "SELECT * FROM pg_logical_slot_get_changes(
                    '$this->replicationSlotName', 
                    NULL, 
                    NULL,
                    'format-version', '1',
                    'pretty-print', 'on',
                    'include-timestamp', 'on',
                    'include-types', 'on',
                    'include-pk', 'on',
                    'include-lsn', 'on'
                )";
                
                $this->logger->info("Starting to fetch changes from replication slot using wal2json");
                
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
                                // 处理 JSON 数据
                                $jsonData = $row['data'];
                                $this->logger->debug("Processing JSON data");
                                
                                // 使用 handleJsonMessage 方法处理 JSON 消息
                                $this->handleJsonMessage($jsonData, $callback);
                            } catch (Exception $e) {
                                $this->logger->error("Error processing change: " . $e->getMessage());
                            }
                        }
                    }
                    
                    // 如果没有变更，等待一段时间再查询
                    if (!$hasChanges) {
                        usleep(100000); // 休眠 100ms
                    }
                }
                
                pg_close($pgsqlConn);
                
            } catch (Exception $e) {
                $this->logger->error("Error in replication process: " . $e->getMessage());
                
                // 尝试重连
                if (!$this->reconnect()) {
                    $this->logger->error("Failed to reconnect after error, stopping replication");
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
     * 强制重新创建复制槽
     *
     * @return bool 成功返回 true，失败返回 false
     */
    public function recreateReplicationSlot(): bool {
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
            
            // 检查复制槽是否存在
            $slotExists = $regularConn->query(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName'"
            )->fetchColumn();
            
            // 如果复制槽存在，删除它
            if ($slotExists) {
                $this->logger->info("删除现有复制槽 {$this->replicationSlotName}");
                $regularConn->exec(
                    "SELECT pg_drop_replication_slot('$this->replicationSlotName')"
                );
            }
            
            // 创建新的复制槽
            $this->logger->info("创建新的复制槽 {$this->replicationSlotName} 使用 wal2json 插件");
            $regularConn->exec(
                "SELECT pg_create_logical_replication_slot('$this->replicationSlotName', 'wal2json')"
            );
            
            return true;
        } catch (Exception $e) {
            $this->logger->error("重新创建复制槽失败: {$e->getMessage()}");
            return false;
        }
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
     * 处理 JSON 格式的消息
     *
     * @param string $jsonData JSON 数据
     * @param callable $callback 回调函数
     * @return void
     */
    private function handleJsonMessage(string $jsonData, callable $callback): void
    {
        // 解析 JSON 数据
        $data = json_decode($jsonData, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->error("Failed to decode JSON data: " . json_last_error_msg());
            return;
        }
        
        // 处理 wal2json 格式的数据
        if (isset($data['change']) && is_array($data['change'])) {
            foreach ($data['change'] as $change) {
                // 使用 LogicalReplicationParser 处理 wal2json 格式的变更
                $parsedData = $this->parser->parseWal2json($change);
                
                // 调用回调函数
                if ($parsedData !== null) {
                    call_user_func($callback, $parsedData, $jsonData);
                }
            }
        }
    }
}