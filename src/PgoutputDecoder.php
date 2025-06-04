<?php

namespace Cooper\PostgresCDC;

class PgoutputDecoder {
    // 消息类型常量
    public const MSG_TYPE_BEGIN = 'B';
    public const MSG_TYPE_COMMIT = 'C';
    public const MSG_TYPE_INSERT = 'I';
    public const MSG_TYPE_UPDATE = 'U';
    public const MSG_TYPE_DELETE = 'D';
    public const MSG_TYPE_RELATION = 'R';
    public const MSG_TYPE_TRUNCATE = 'T';
    public const MSG_TYPE_TYPE = 'Y';
    public const MSG_TYPE_ORIGIN = 'O';

    /**
     * 解码二进制数据
     *
     * @param string $binaryData 从PostgreSQL接收的二进制数据
     * @return array 解析后的数据
     */
    public function decode(string $binaryData): array {
        if (empty($binaryData)) {
            return ['type' => 'empty'];
        }

        // 确保数据至少有一个字节
        if (strlen($binaryData) < 1) {
            return ['type' => 'error', 'message' => '数据长度不足'];
        }

        $messageType = $binaryData[0];
        
        try {
            switch ($messageType) {
                case self::MSG_TYPE_BEGIN:
                    return $this->decodeBegin($binaryData);
                case self::MSG_TYPE_COMMIT:
                    return $this->decodeCommit($binaryData);
                case self::MSG_TYPE_INSERT:
                    return $this->decodeInsert($binaryData);
                case self::MSG_TYPE_UPDATE:
                    return $this->decodeUpdate($binaryData);
                case self::MSG_TYPE_DELETE:
                    return $this->decodeDelete($binaryData);
                case self::MSG_TYPE_RELATION:
                    return $this->decodeRelation($binaryData);
                case self::MSG_TYPE_TRUNCATE:
                    return $this->decodeTruncate($binaryData);
                case self::MSG_TYPE_TYPE:
                    return $this->decodeType($binaryData);
                case self::MSG_TYPE_ORIGIN:
                    return $this->decodeOrigin($binaryData);
                default:
                    return [
                        'type' => 'unknown',
                        'message_type' => bin2hex($messageType),
                        'raw_hex' => bin2hex(substr($binaryData, 0, 50)) . '...'
                    ];
            }
        } catch (\Exception $e) {
            return [
                'type' => 'error',
                'message' => '解码错误: ' . $e->getMessage(),
                'raw_hex' => bin2hex(substr($binaryData, 0, 50)) . '...'
            ];
        }
    }

    /**
     * 解码BEGIN消息
     */
    private function decodeBegin(string $data): array {
        $unpacked1 = unpack('J', substr($data, 1, 8));
        $lsn = $unpacked1[1] ?? 0;
        
        $unpacked2 = unpack('J', substr($data, 9, 8));
        $timestamp = $unpacked2[1] ?? 0;
        
        $unpacked3 = unpack('N', substr($data, 17, 4));
        $xid = $unpacked3[1] ?? 0;

        // PostgreSQL时间戳是从2000-01-01开始的微秒数
        $formattedTimestamp = $this->formatPgTimestamp($timestamp);

        return [
            'type' => 'begin',
            'lsn' => $lsn,
            'timestamp' => $timestamp,
            'timestamp_formatted' => $formattedTimestamp,
            'xid' => $xid
        ];
    }

    /**
     * 解码COMMIT消息
     */
    private function decodeCommit(string $data): array {
        $unpacked1 = unpack('C', substr($data, 1, 1));
        $flags = $unpacked1[1] ?? 0;
        
        $unpacked2 = unpack('J', substr($data, 2, 8));
        $lsn = $unpacked2[1] ?? 0;
        
        $unpacked3 = unpack('J', substr($data, 10, 8));
        $endLsn = $unpacked3[1] ?? 0;
        
        $unpacked4 = unpack('J', substr($data, 18, 8));
        $timestamp = $unpacked4[1] ?? 0;

        // PostgreSQL时间戳是从2000-01-01开始的微秒数
        $formattedTimestamp = $this->formatPgTimestamp($timestamp);

        return [
            'type' => 'commit',
            'flags' => $flags,
            'lsn' => $lsn,
            'end_lsn' => $endLsn,
            'timestamp' => $timestamp,
            'timestamp_formatted' => $formattedTimestamp
        ];
    }

    /**
     * 解码INSERT消息
     */
    private function decodeInsert(string $data): array {
        $unpacked = unpack('N', substr($data, 1, 4));
        $relationId = $unpacked[1] ?? 0;
        $tupleData = substr($data, 5);
        
        $result = [
            'type' => 'insert',
            'relation_id' => $relationId,
            'tuple_data' => $this->decodeTupleData($tupleData)
        ];

        return $result;
    }

    /**
     * 解码UPDATE消息
     */
    private function decodeUpdate(string $data): array {
        $unpacked = unpack('N', substr($data, 1, 4));
        $relationId = $unpacked[1] ?? 0;
        $offset = 5;
        
        // 检查是否有旧元组数据
        $hasOldTuple = ord($data[$offset]) === 'O';
        $offset++;
        
        $oldTupleData = null;
        if ($hasOldTuple) {
            // 解析旧元组数据
            $unpacked = unpack('N', substr($data, $offset, 4));
            $oldTupleLength = $unpacked[1] ?? 0;
            $offset += 4;
            $oldTupleData = $this->decodeTupleData(substr($data, $offset, $oldTupleLength));
            $offset += $oldTupleLength;
        }
        
        // 解析新元组数据
        $newTupleData = $this->decodeTupleData(substr($data, $offset));
        
        return [
            'type' => 'update',
            'relation_id' => $relationId,
            'has_old_tuple' => $hasOldTuple,
            'old_tuple_data' => $oldTupleData,
            'new_tuple_data' => $newTupleData
        ];
    }

    /**
     * 解码DELETE消息
     */
    private function decodeDelete(string $data): array {
        $unpacked = unpack('N', substr($data, 1, 4));
        $relationId = $unpacked[1] ?? 0;
        $tupleData = substr($data, 5);
        
        return [
            'type' => 'delete',
            'relation_id' => $relationId,
            'tuple_data' => $this->decodeTupleData($tupleData)
        ];
    }

    /**
     * 解码RELATION消息
     */
    private function decodeRelation(string $data): array {
        $offset = 1;
        $unpacked = unpack('N', substr($data, $offset, 4));
        $relationId = $unpacked[1] ?? 0;
        $offset += 4;
        
        // 读取命名空间（schema）
        $namespaceLength = strpos(substr($data, $offset), "\0");
        $namespace = substr($data, $offset, $namespaceLength);
        $offset += $namespaceLength + 1; // +1 for null terminator
        
        // 读取表名
        $relationNameLength = strpos(substr($data, $offset), "\0");
        $relationName = substr($data, $offset, $relationNameLength);
        $offset += $relationNameLength + 1;
        
        // 读取复制标识
        $replicaIdentity = ord($data[$offset]);
        $offset += 1;
        
        // 读取列数
        $unpacked = unpack('n', substr($data, $offset, 2));
        $columnCount = $unpacked[1] ?? 0;
        $offset += 2;
        
        // 读取列信息
        $columns = [];
        for ($i = 0; $i < $columnCount; $i++) {
            $flags = ord($data[$offset]);
            $offset += 1;
            
            // 读取列名
            $columnNameLength = strpos(substr($data, $offset), "\0");
            $columnName = substr($data, $offset, $columnNameLength);
            $offset += $columnNameLength + 1;
            
            // 读取数据类型
            $unpacked = unpack('N', substr($data, $offset, 4));
            $dataTypeId = $unpacked[1] ?? 0;
            $offset += 4;
            
            // 读取类型修饰符
            $unpacked = unpack('N', substr($data, $offset, 4));
            $typeModifier = $unpacked[1] ?? 0;
            $offset += 4;
            
            $columns[] = [
                'name' => $columnName,
                'flags' => $flags,
                'data_type_id' => $dataTypeId,
                'type_modifier' => $typeModifier,
                'is_key' => ($flags & 1) === 1
            ];
        }
        
        return [
            'type' => 'relation',
            'relation_id' => $relationId,
            'namespace' => $namespace,
            'relation_name' => $relationName,
            'replica_identity' => $this->getReplicaIdentityName($replicaIdentity),
            'columns' => $columns
        ];
    }

    /**
     * 解码TRUNCATE消息
     */
    private function decodeTruncate(string $data): array {
        $offset = 1;
        $unpacked = unpack('N', substr($data, $offset, 4));
        $flags = $unpacked[1] ?? 0;
        $offset += 4;
        
        $unpacked = unpack('n', substr($data, $offset, 2));
        $relationCount = $unpacked[1] ?? 0;
        $offset += 2;
        
        $relations = [];
        for ($i = 0; $i < $relationCount; $i++) {
            $unpacked = unpack('N', substr($data, $offset, 4));
            $relations[] = $unpacked[1] ?? 0;
            $offset += 4;
        }
        
        return [
            'type' => 'truncate',
            'flags' => $flags,
            'cascade' => ($flags & 1) === 1,
            'restart_identity' => ($flags & 2) === 2,
            'relations' => $relations
        ];
    }

    /**
     * 解码TYPE消息
     */
    private function decodeType(string $data): array {
        $offset = 1;
        $unpacked = unpack('N', substr($data, $offset, 4));
        $typeId = $unpacked[1] ?? 0;
        $offset += 4;
        
        // 读取命名空间（schema）
        $namespaceLength = strpos(substr($data, $offset), "\0");
        $namespace = substr($data, $offset, $namespaceLength);
        $offset += $namespaceLength + 1;
        
        // 读取类型名
        $typeNameLength = strpos(substr($data, $offset), "\0");
        $typeName = substr($data, $offset, $typeNameLength);
        
        return [
            'type' => 'type',
            'type_id' => $typeId,
            'namespace' => $namespace,
            'type_name' => $typeName
        ];
    }

    /**
     * 解码ORIGIN消息
     */
    private function decodeOrigin(string $data): array {
        $unpacked = unpack('J', substr($data, 1, 8));
        $lsn = $unpacked[1] ?? 0;
        $name = substr($data, 9, -1); // 去掉末尾的null终止符
        
        return [
            'type' => 'origin',
            'lsn' => $lsn,
            'name' => $name
        ];
    }

    /**
     * 解码元组数据
     */
    private function decodeTupleData(string $data): array {
        $result = [];
        $offset = 0;
        
        // 检查数据是否为空或长度不足
        if (empty($data) || strlen($data) < 2) {
            return $result;
        }
        
        $unpacked = unpack('n', substr($data, $offset, 2));
        $columnCount = $unpacked[1] ?? 0;
        $offset += 2;
        
        for ($i = 0; $i < $columnCount; $i++) {
            // 检查是否有足够的数据读取列类型
            if ($offset >= strlen($data)) {
                break;
            }
            
            $columnType = ord($data[$offset]);
            $offset += 1;
            
            switch ($columnType) {
                case 110: // 'n' - null
                    $result[] = null;
                    break;
                    
                case 117: // 'u' - unchanged toast
                    $result[] = ['type' => 'unchanged_toast'];
                    break;
                    
                case 116: // 't' - text
                    // 检查是否有足够的数据来读取长度
                    if ($offset + 4 > strlen($data)) {
                        $result[] = null;
                        break;
                    }
                    $unpacked = unpack('N', substr($data, $offset, 4));
                    $valueLength = $unpacked[1] ?? 0;
                    $offset += 4;
                    
                    // 检查是否有足够的数据来读取值
                    if ($offset + $valueLength > strlen($data)) {
                        $result[] = null;
                        break;
                    }
                    $value = substr($data, $offset, $valueLength);
                    $offset += $valueLength;
                    $result[] = $value;
                    break;

                case 0: // 二进制格式 (0x00)
                    // 某些情况下PostgreSQL会使用二进制格式发送数据
                    // 检查是否有足够的数据来读取长度
                    if ($offset + 4 > strlen($data)) {
                        $result[] = null;
                        break;
                    }
                    $unpacked = unpack('N', substr($data, $offset, 4));
                    $valueLength = $unpacked[1] ?? 0;
                    $offset += 4;
                    if ($valueLength == 0xFFFFFFFF) {
                        // -1 表示NULL值
                        $result[] = null;
                    } else {
                        // 检查是否有足够的数据来读取值
                        if ($offset + $valueLength > strlen($data)) {
                            $result[] = null;
                            break;
                        }
                        $value = substr($data, $offset, $valueLength);
                        $offset += $valueLength;
                        // 尝试将二进制数据转换为可读格式
                        $result[] = $this->convertBinaryValue($value);
                    }
                    break;
                    
                default:
                    // 记录未知类型并继续处理
                    $hexCode = dechex($columnType);
                    $result[] = ['type' => 'unknown', 'code' => $hexCode];
                    
                    // 尝试跳过未知类型的数据（假设有4字节长度头）
                    if ($offset + 4 <= strlen($data)) {
                        $unpacked = unpack('N', substr($data, $offset, 4));
                        $valueLength = $unpacked[1] ?? 0;
                        $offset += 4;
                        if ($valueLength > 0 && $offset + $valueLength <= strlen($data)) {
                            $offset += $valueLength;
                        }
                    }
            }
        }
        
        return $result;
    }
    
    /**
     * 尝试将二进制值转换为可读格式
     */
    private function convertBinaryValue(string $binary): mixed {
        // 检查数据是否为空
        if (empty($binary)) {
            return '0x';
        }
        
        // 尝试各种常见的数据类型转换
        
        // 尝试作为整数解析
        if (strlen($binary) <= 8) {
            if (strlen($binary) == 4) {
                $unpacked = unpack('l', $binary);
                $int = $unpacked[1] ?? false;
                if ($int !== false) {
                    return $int;
                }
            } elseif (strlen($binary) == 8) {
                $unpacked = unpack('q', $binary);
                $int = $unpacked[1] ?? false;
                if ($int !== false) {
                    return $int;
                }
            }
        }
        
        // 尝试作为浮点数解析
        if (strlen($binary) == 4) {
            $unpacked = unpack('f', $binary);
            $float = $unpacked[1] ?? NAN;
            if (!is_nan($float)) {
                return $float;
            }
        } elseif (strlen($binary) == 8) {
            $unpacked = unpack('d', $binary);
            $double = $unpacked[1] ?? NAN;
            if (!is_nan($double)) {
                return $double;
            }
        }
        
        // 尝试作为日期/时间解析
        if (strlen($binary) == 8) {
            // PostgreSQL的时间戳是从2000-01-01开始的微秒数
            $unpacked = unpack('q', $binary);
            $microseconds = $unpacked[1] ?? false;
            if ($microseconds !== false && $microseconds > 0) {
                try {
                    // 转换为Unix时间戳
                    $timestamp = strtotime('2000-01-01') + floor($microseconds / 1000000);
                    return date('Y-m-d H:i:s', $timestamp);
                } catch (\Exception $e) {
                    // 转换失败，继续尝试其他格式
                }
            }
        }
        
        // 尝试作为布尔值解析
        if (strlen($binary) == 1) {
            $bool = ord($binary);
            if ($bool === 0) return false;
            if ($bool === 1) return true;
        }
        
        // 如果无法识别，返回十六进制表示
        return '0x' . bin2hex($binary);
    }

    /**
     * 获取复制标识名称
     */
    private function getReplicaIdentityName(int $code): string {
        switch ($code) {
            case 'd': return 'default';
            case 'n': return 'nothing';
            case 'f': return 'full';
            case 'i': return 'index';
            default: return 'unknown';
        }
    }

    /**
     * 格式化PostgreSQL时间戳
     * PostgreSQL WAL时间戳是从2000-01-01 00:00:00 UTC开始的微秒数
     */
    private function formatPgTimestamp($microseconds): string {
        try {
            // PostgreSQL纪元（2000-01-01 00:00:00 UTC）
            $pgEpoch = strtotime('2000-01-01 00:00:00 UTC');
            
            // 转换微秒为秒并添加到PostgreSQL纪元
            $timestamp = $pgEpoch + (int)($microseconds / 1000000);
            
            // 格式化为可读的日期时间
            return date('Y-m-d H:i:s', $timestamp);
        } catch (\Exception $e) {
            // 如果转换失败，返回原始值
            return "timestamp:{$microseconds}";
        }
    }
} 