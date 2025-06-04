<?php

namespace Cooper\PostgresCDC;

use Exception;
use JsonException;
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use PDO;
use PDOException;

/**
 * PostgreSQL 逻辑复制客户端
 * 
 * 使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出，将变更数据转换为 PHP 数组
 */
class PostgresLogicalReplication
{
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

    /**
     * 构造函数
     *
     * @param array $dbConfig 数据库配置
     * @param Logger|null $logger 日志记录器
     */
    public function __construct(array $dbConfig, ?Logger $logger = null)
    {
        $this->dbConfig = $dbConfig;
        $this->replicationSlotName = $dbConfig['replication_slot_name'] ?? 'php_logical_slot';
        $this->publicationName = $dbConfig['publication_name'] ?? 'php_publication';
        $this->lastHeartbeat = time();
        $this->logger = $logger ?? $this->createDefaultLogger();
    }

    /**
     * 连接数据库
     *
     * @return bool 连接成功返回 true，失败返回 false
     */
    public function connect(): bool
    {
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
            $this->logger->error("连接失败: {$e->getMessage()}");
            return false;
        }
    }

    /**
     * 设置复制环境
     *
     * @return bool 设置成功返回 true，失败返回 false
     */
    public function setupReplication(): bool
    {
        try {
            $regularConn = $this->createRegularConnection();

            // 检查并设置 wal_level
            $walLevel = $regularConn->query("SHOW wal_level")->fetchColumn();
            if ($walLevel !== 'logical') {
                throw new Exception("WAL level 必须在 postgresql.conf 中设置为 'logical'");
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
            $this->logger->error("设置复制失败: {$e->getMessage()}");
            return false;
        }
    }

    /**
     * 开始复制过程
     *
     * @param callable $callback 处理变更数据的回调函数
     * @return bool 复制成功返回 true，失败返回 false
     */
    public function startReplication(callable $callback): bool
    {
        // 检查PHP是否安装了必要的PostgreSQL扩展
        if (!extension_loaded('pgsql')) {
            $this->logger->error("未加载 PostgreSQL 扩展");
            return false;
        }

        // 添加一个终止标志
        $this->isRunning = true;

        // 注册信号处理器
        $this->setupSignalHandlers();

        while ($this->isRunning) {
            try {
                if (!$this->checkConnection()) {
                    if (!$this->reconnect()) {
                        throw new Exception("无法维持连接");
                    }
                }

                // 建立专用的复制连接
                $pgsqlConn = $this->createReplicationConnection();
                if (!$pgsqlConn) {
                    throw new Exception("无法建立 PostgreSQL 连接");
                }

                // 验证复制槽
                $this->validateReplicationSlot($pgsqlConn);

                // 使用 wal2json 插件获取 JSON 格式的变更数据
                $query = $this->buildReplicationQuery();

                $this->logger->info("开始从复制槽获取变更数据，使用 wal2json");

                // 主循环
                while ($this->isRunning) {
                    // 允许信号处理
                    if (function_exists('pcntl_signal_dispatch')) {
                        pcntl_signal_dispatch();
                    }

                    // 获取变更数据
                    $result = pg_query($pgsqlConn, $query);
                    if (!$result) {
                        throw new Exception("获取变更失败: " . pg_last_error($pgsqlConn));
                    }

                    $hasChanges = false;

                    // 处理所有变更
                    while ($row = pg_fetch_assoc($result)) {
                        $hasChanges = true;
                        if (isset($row['data'])) {
                            $this->logger->debug("接收到变更数据");
                            try {
                                $this->handleJsonMessage($row['data'], $callback);
                            } catch (Exception $e) {
                                $this->logger->error("处理变更出错: " . $e->getMessage());
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
                $this->logger->error("复制过程出错: " . $e->getMessage());

                // 尝试重连
                if (!$this->reconnect()) {
                    $this->logger->error("错误后无法重连，停止复制");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 关闭连接
     */
    public function close(): void
    {
        // 设置终止标志
        $this->isRunning = false;

        // 关闭数据库连接
        if ($this->conn) {
            $this->conn = null;
        }

        $this->logger->info("连接已关闭");
    }

    /**
     * 设置心跳间隔
     *
     * @param int $seconds 心跳间隔（秒）
     */
    public function setHeartbeatInterval(int $seconds): void
    {
        $this->heartbeatInterval = $seconds;
    }

    /**
     * 设置最大重连次数
     *
     * @param int $attempts 最大重连次数
     */
    public function setMaxReconnectAttempts(int $attempts): void
    {
        $this->maxReconnectAttempts = $attempts;
    }

    /**
     * 设置重连延迟
     *
     * @param int $seconds 重连延迟（秒）
     */
    public function setReconnectDelay(int $seconds): void
    {
        $this->reconnectDelay = $seconds;
    }

    /**
     * 强制重新创建复制槽
     *
     * @return bool 成功返回 true，失败返回 false
     */
    public function recreateReplicationSlot(): bool
    {
        try {
            $regularConn = $this->createRegularConnection();

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
     * 创建标准数据库连接（非复制连接）
     *
     * @return PDO
     */
    private function createRegularConnection(): PDO
    {
        $regularDsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;application_name=%s',
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['dbname'],
            $this->dbConfig['application_name'] ?? 'php_logical_replication'
        );

        return new PDO(
            $regularDsn,
            $this->dbConfig['user'],
            $this->dbConfig['password'],
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
            ]
        );
    }

    /**
     * 创建复制连接
     *
     * @return resource|false
     */
    private function createReplicationConnection()
    {
        $connString = sprintf(
            "host=%s port=%s dbname=%s user=%s password=%s application_name=%s",
            $this->dbConfig['host'],
            $this->dbConfig['port'],
            $this->dbConfig['dbname'],
            $this->dbConfig['user'],
            $this->dbConfig['password'],
            $this->dbConfig['application_name'] ?? 'php_logical_replication'
        );

        return pg_connect($connString);
    }

    /**
     * 验证复制槽
     *
     * @param resource $pgsqlConn PostgreSQL 连接
     * @throws Exception 如果复制槽不存在或使用了错误的插件
     */
    private function validateReplicationSlot($pgsqlConn): void
    {
        // 检查是否存在复制槽
        $slotExists = pg_fetch_result(
            pg_query($pgsqlConn, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName')"),
            0, 0
        );

        if ($slotExists !== 't' && $slotExists !== 'true') {
            throw new Exception("复制槽 '$this->replicationSlotName' 不存在");
        }

        // 检查复制槽是否使用 wal2json 插件
        $pluginName = pg_fetch_result(
            pg_query($pgsqlConn, "SELECT plugin FROM pg_replication_slots WHERE slot_name = '$this->replicationSlotName'"),
            0, 0
        );

        if ($pluginName !== 'wal2json') {
            throw new Exception("复制槽 '$this->replicationSlotName' 使用的是 '$pluginName' 插件而不是 'wal2json'");
        }
    }

    /**
     * 构建复制查询
     *
     * @return string
     */
    private function buildReplicationQuery(): string
    {
        return "SELECT * FROM pg_logical_slot_get_changes(
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
    }

    /**
     * 设置信号处理器
     */
    private function setupSignalHandlers(): void
    {
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGINT, function () {
                $this->isRunning = false;
            });
            pcntl_signal(SIGTERM, function () {
                $this->isRunning = false;
            });
            if (function_exists('pcntl_async_signals')) {
                pcntl_async_signals(true);
            }
        }
    }

    /**
     * 发送心跳
     *
     * @return bool 心跳成功返回 true，失败返回 false
     */
    private function sendHeartbeat(): bool
    {
        try {
            $this->conn?->exec("SELECT 1"); // 简单的心跳查询
            $this->lastHeartbeat = time();
            return true;
        } catch (Exception $e) {
            $this->logger->error("心跳失败: {$e->getMessage()}");
            return false;
        }
    }

    /**
     * 检查连接状态
     *
     * @return bool 连接正常返回 true，失败返回 false
     */
    private function checkConnection(): bool
    {
        if (time() - $this->lastHeartbeat > $this->heartbeatInterval) {
            return $this->sendHeartbeat();
        }
        return true;
    }

    /**
     * 尝试重新连接
     *
     * @return bool 重连成功返回 true，失败返回 false
     */
    private function reconnect(): bool
    {
        for ($attempt = 1; $attempt <= $this->maxReconnectAttempts; $attempt++) {
            $this->logger->info("尝试重连 (第 $attempt 次，共 {$this->maxReconnectAttempts} 次)...");

            if ($this->connect() && $this->setupReplication()) {
                $this->logger->info("重连成功");
                return true;
            }

            if ($attempt < $this->maxReconnectAttempts) {
                sleep($this->reconnectDelay);
            }
        }

        $this->logger->error("尝试 {$this->maxReconnectAttempts} 次后重连失败");
        return false;
    }

    /**
     * 创建默认日志记录器
     *
     * @return Logger
     */
    private function createDefaultLogger(): Logger
    {
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
        try {
            $data = json_decode($jsonData, true, 512, JSON_THROW_ON_ERROR);
            $callback($data, $jsonData);
        } catch (JsonException $e) {
            $this->logger->error("JSON 数据解析失败: {$e->getMessage()}");
        }
    }
}
