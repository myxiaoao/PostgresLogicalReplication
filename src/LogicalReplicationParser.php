<?php

namespace Cooper\PostgresCDC;

use Exception;

class LogicalReplicationParser {
    private RelationMapping $relationMapping;
    
    public function __construct() {
        $this->relationMapping = new RelationMapping();
    }
    
    /**
     * 解析 wal2json 格式的数据
     *
     * @param array $data wal2json 格式的数据
     * @return array|null 解析后的数据，如果无法解析则返回 null
     */
    public function parseWal2json(array $data): ?array
    {
        try {
            // 处理消息类型
            if (isset($data['kind']) && $data['kind'] === 'message') {
                return [
                    'type' => 'message',
                    'transactional' => $data['transactional'] ?? false,
                    'prefix' => $data['prefix'] ?? '',
                    'content' => $data['content'] ?? ''
                ];
            }
            
            // 处理数据操作类型
            if (!isset($data['kind'])) {
                return null;
            }
            
            $result = [
                'type' => $data['kind'],
                'schema' => $data['schema'] ?? 'public',
                'table' => $data['table'] ?? null,
            ];
            
            // 处理 begin 和 commit 消息（通过 action 字段）
            if (isset($data['action'])) {
                if ($data['action'] === 'B') {
                    $result['type'] = 'begin';
                    if (isset($data['xid'])) {
                        $result['xid'] = $data['xid'];
                    }
                    if (isset($data['timestamp'])) {
                        $result['timestamp'] = $data['timestamp'];
                        $result['timestamp_formatted'] = $data['timestamp'];
                    }
                } elseif ($data['action'] === 'C') {
                    $result['type'] = 'commit';
                    if (isset($data['lsn'])) {
                        $result['lsn'] = $data['lsn'];
                    }
                    if (isset($data['timestamp'])) {
                        $result['timestamp'] = $data['timestamp'];
                        $result['timestamp_formatted'] = $data['timestamp'];
                    }
                }
            }
            
            // 添加关系ID（如果存在）
            if (isset($data['relation_id'])) {
                $result['relation_id'] = $data['relation_id'];
            }
            
            // 处理插入操作
            if ($data['kind'] === 'insert') {
                if (isset($data['columnnames'], $data['columntypes'], $data['columnvalues'])) {
                    $columnData = [];
                    foreach ($data['columnnames'] as $index => $name) {
                        $columnData[$name] = $data['columnvalues'][$index] ?? null;
                    }
                    $result['data'] = $columnData;
                }
                
                // 添加主键信息
                if (isset($data['pk'])) {
                    $result['primary_keys'] = array_column($data['pk'], 'name');
                }
            }
            
            // 处理更新操作
            elseif ($data['kind'] === 'update') {
                if (isset($data['columnnames'], $data['columntypes'], $data['columnvalues'])) {
                    $newData = [];
                    foreach ($data['columnnames'] as $index => $name) {
                        $newData[$name] = $data['columnvalues'][$index] ?? null;
                    }
                    $result['new_data'] = $newData;
                    $result['has_old_tuple'] = isset($data['oldkeys']);
                }
                
                // 处理旧数据
                if (isset($data['oldkeys'])) {
                    $oldData = [];
                    if (isset($data['oldkeys']['keynames'], $data['oldkeys']['keyvalues'])) {
                        foreach ($data['oldkeys']['keynames'] as $index => $name) {
                            $oldData[$name] = $data['oldkeys']['keyvalues'][$index] ?? null;
                        }
                    }
                    $result['old_data'] = $oldData;
                }
                
                // 添加主键信息
                if (isset($data['pk'])) {
                    $result['primary_keys'] = array_column($data['pk'], 'name');
                } elseif (isset($data['oldkeys']['keynames'])) {
                    $result['primary_keys'] = $data['oldkeys']['keynames'];
                }
            }
            
            // 处理删除操作
            elseif ($data['kind'] === 'delete') {
                if (isset($data['oldkeys'])) {
                    $oldData = [];
                    if (isset($data['oldkeys']['keynames'], $data['oldkeys']['keyvalues'])) {
                        foreach ($data['oldkeys']['keynames'] as $index => $name) {
                            $oldData[$name] = $data['oldkeys']['keyvalues'][$index] ?? null;
                        }
                    }
                    $result['data'] = $oldData;
                    
                    // 添加主键信息
                    if (isset($data['oldkeys']['keynames'])) {
                        $result['primary_keys'] = $data['oldkeys']['keynames'];
                    }
                }
            }
            
            // 处理截断操作
            elseif ($data['kind'] === 'truncate') {
                // 不需要额外处理
            }
            
            // 处理关系定义
            elseif ($data['kind'] === 'relation') {
                $result['relation_id'] = $data['relation_id'] ?? null;
                $result['namespace'] = $data['schema'] ?? $data['namespace'] ?? 'public';
                $result['relation_name'] = $data['relation_name'] ?? $data['table'] ?? null;
                $result['replica_identity'] = $data['replica_identity'] ?? null;
                
                // 处理列信息
                if (isset($data['columns']) && is_array($data['columns'])) {
                    $result['columns'] = $data['columns'];
                } else {
                    // 尝试从 columnnames 和 columntypes 构建列信息
                    if (isset($data['columnnames'], $data['columntypes'])) {
                        $columns = [];
                        foreach ($data['columnnames'] as $index => $name) {
                            $columns[] = [
                                'name' => $name,
                                'data_type_id' => $data['columntypes'][$index] ?? null,
                                'is_key' => false // 默认不是主键
                            ];
                        }
                        $result['columns'] = $columns;
                    }
                }
            }
            
            // 添加 LSN 信息
            if (isset($data['nextlsn'])) {
                $result['lsn'] = $data['nextlsn'];
            }
            
            // 添加时间戳
            if (isset($data['timestamp'])) {
                $result['timestamp'] = $data['timestamp'];
                $result['timestamp_formatted'] = $data['timestamp'];
            }
            
            return $result;
        } catch (\Exception $e) {
            return [
                'type' => 'error',
                'message' => "解析 wal2json 数据失败: " . $e->getMessage()
            ];
        }
    }
    
    /**
     * 获取关系映射对象
     *
     * @return RelationMapping
     */
    public function getRelationMapping(): RelationMapping {
        return $this->relationMapping;
    }
    
    /**
     * 清除所有映射数据
     *
     * @return void
     */
    public function clearMappings(): void {
        $this->relationMapping->clear();
    }
} 