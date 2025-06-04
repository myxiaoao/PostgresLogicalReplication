<?php

namespace Cooper\PostgresCDC;

use Exception;

class LogicalReplicationParser {
    private PgoutputDecoder $decoder;
    private RelationMapping $relationMapping;
    
    public function __construct() {
        $this->decoder = new PgoutputDecoder();
        $this->relationMapping = new RelationMapping();
    }
    
    /**
     * 解析二进制数据
     *
     * @param string $binaryData 从PostgreSQL接收的二进制数据
     * @return array 解析后的数据
     * @throws Exception 如果解析失败
     */
    public function parse(string $binaryData): array {
        try {
            // 检查数据是否为空
            if (empty($binaryData)) {
                return ['type' => 'empty', 'message' => '接收到空数据'];
            }
            
            $decodedData = $this->decoder->decode($binaryData);
            
            // 检查解码结果是否为错误
            if (isset($decodedData['type']) && $decodedData['type'] === 'error') {
                return $decodedData; // 直接返回错误信息
            }
            
            // 处理关系和类型消息，更新映射
            if ($decodedData['type'] === 'relation') {
                $this->relationMapping->addRelation(
                    $decodedData['relation_id'],
                    $decodedData['namespace'],
                    $decodedData['relation_name'],
                    $decodedData['columns']
                );
            } elseif ($decodedData['type'] === 'type') {
                $this->relationMapping->addType(
                    $decodedData['type_id'],
                    $decodedData['namespace'],
                    $decodedData['type_name']
                );
            }
            
            // 增强数据操作消息
            if (in_array($decodedData['type'], ['insert', 'update', 'delete'])) {
                $decodedData = $this->enhanceDataOperationMessage($decodedData);
            }
            
            return $decodedData;
        } catch (Exception $e) {
            return [
                'type' => 'error',
                'message' => "解析二进制数据失败: " . $e->getMessage(),
                'raw_hex' => !empty($binaryData) ? bin2hex(substr($binaryData, 0, 50)) . '...' : '空数据'
            ];
        }
    }
    
    /**
     * 增强数据操作消息（插入、更新、删除）
     *
     * @param array $message 解码后的消息
     * @return array 增强后的消息
     */
    private function enhanceDataOperationMessage(array $message): array {
        $relationId = $message['relation_id'];
        
        // 添加表名信息
        $message['table'] = $this->relationMapping->getFullTableName($relationId);
        
        // 处理元组数据
        if ($message['type'] === 'insert') {
            $message['data'] = $this->relationMapping->mapTupleDataToColumns($relationId, $message['tuple_data']);
            unset($message['tuple_data']);
        } elseif ($message['type'] === 'update') {
            if ($message['has_old_tuple'] && $message['old_tuple_data'] !== null) {
                $message['old_data'] = $this->relationMapping->mapTupleDataToColumns($relationId, $message['old_tuple_data']);
            }
            $message['new_data'] = $this->relationMapping->mapTupleDataToColumns($relationId, $message['new_tuple_data']);
            unset($message['old_tuple_data'], $message['new_tuple_data']);
        } elseif ($message['type'] === 'delete') {
            try {
                // 处理DELETE操作的元组数据
                $mappedData = $this->relationMapping->mapTupleDataToColumns($relationId, $message['tuple_data']);
                
                // 过滤掉未知类型的数据
                $filteredData = [];
                foreach ($mappedData as $key => $value) {
                    // 如果值是数组且有type字段，可能是未知类型
                    if (is_array($value) && isset($value['type']) && $value['type'] === 'unknown') {
                        // 跳过未知类型
                        continue;
                    }
                    $filteredData[$key] = $value;
                }
                
                $message['data'] = $filteredData;
            } catch (\Exception $e) {
                // 如果映射失败，保留原始数据
                $message['data'] = $message['tuple_data'];
                $message['mapping_error'] = $e->getMessage();
            }
            unset($message['tuple_data']);
        }
        
        // 添加主键信息
        $message['primary_keys'] = $this->relationMapping->getPrimaryKeyColumns($relationId);
        
        return $message;
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