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
    
    public function __construct(array $dbConfig, ?Logger $logger = null) {
        $this->dbConfig = $dbConfig;
        $this->replicationSlotName = $dbConfig['replication_slot_name'] ?? 'php_logical_slot';
        $this->publicationName = $dbConfig['publication_name'] ?? 'php_publication';
        $this->lastHeartbeat = time();
        $this->logger = $logger ?? $this->createDefaultLogger();
    }
    
    public function connect(): bool {
        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;options=\'--replication=database\'',
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['dbname']
        );
        
        try {
            $this->conn = new PDO(
                $dsn,
                $this->dbConfig['user'],
                $this->dbConfig['password'],
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );
            return true;
        } catch (PDOException $e) {
            $this->logger->error("Connection failed: {$e->getMessage()}");
            return false;
        }
    }
    
    public function setupReplication(): bool {
        try {
            // 检查并设置 wal_level
            $walLevel = $this->conn?->query("SHOW wal_level")->fetchColumn();
            if ($walLevel !== 'logical') {
                throw new Exception("WAL level must be set to 'logical' in postgresql.conf");
            }
            
            // 创建复制槽
            $this->conn?->exec(
                "SELECT pg_create_logical_replication_slot(" .
                "'$this->replicationSlotName', 'pgoutput') WHERE NOT EXISTS (" .
                "SELECT 1 FROM pg_replication_slots " .
                "WHERE slot_name = '$this->replicationSlotName')"
            );
            
            // 创建发布
            $this->conn?->exec(
                "CREATE PUBLICATION $this->publicationName " .
                "FOR ALL TABLES IF NOT EXISTS"
            );
            
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
        while (true) {
            try {
                if (!$this->checkConnection()) {
                    if (!$this->reconnect()) {
                        throw new Exception("Failed to maintain connection");
                    }
                }
                
                $stmt = $this->conn?->query(
                    "START_REPLICATION SLOT $this->replicationSlotName " .
                    "LOGICAL 0/0 (proto_version '1', publication_names '$this->publicationName')"
                );
                
                while ($row = $stmt?->fetch(PDO::FETCH_ASSOC)) {
                    if (isset($row['data'])) {
                        $change = json_decode($row['data'], true);
                        $callback($change);
                    }
                    
                    // 在处理消息之间检查连接状态
                    if (!$this->checkConnection()) {
                        throw new Exception("Connection lost during replication");
                    }
                }
            } catch (Exception $e) {
                $this->logger->error("Replication error: {$e->getMessage()}");
                
                if (!$this->reconnect()) {
                    $this->logger->error("Fatal error: Unable to recover connection");
                    return false;
                }
            }
        }
    }
    
    public function close(): void {
        if ($this->conn) {
            $this->conn = null;
        }
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
}