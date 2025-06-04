<?php

namespace Cooper\PostgresCDC;

class RelationMapping {
    /**
     * 关系映射数组，格式为：
     * [
     *   关系ID => [
     *     'namespace' => 命名空间/模式,
     *     'relation_name' => 表名,
     *     'columns' => [
     *       [
     *         'name' => 列名,
     *         'flags' => 标志,
     *         'data_type_id' => 数据类型ID,
     *         'type_modifier' => 类型修饰符,
     *         'is_key' => 是否为主键
     *       ],
     *       ...
     *     ]
     *   ],
     *   ...
     * ]
     */
    private array $relations = [];
    
    /**
     * 数据类型映射，格式为：
     * [
     *   类型ID => [
     *     'namespace' => 命名空间/模式,
     *     'type_name' => 类型名称
     *   ],
     *   ...
     * ]
     */
    private array $types = [];
    
    /**
     * 添加关系信息
     *
     * @param int $relationId 关系ID
     * @param string $namespace 命名空间/模式
     * @param string $relationName 表名
     * @param array $columns 列信息数组
     * @return void
     */
    public function addRelation(int $relationId, string $namespace, string $relationName, array $columns): void {
        $this->relations[$relationId] = [
            'namespace' => $namespace,
            'relation_name' => $relationName,
            'columns' => $columns
        ];
    }
    
    /**
     * 添加数据类型信息
     *
     * @param int $typeId 类型ID
     * @param string $namespace 命名空间/模式
     * @param string $typeName 类型名称
     * @return void
     */
    public function addType(int $typeId, string $namespace, string $typeName): void {
        $this->types[$typeId] = [
            'namespace' => $namespace,
            'type_name' => $typeName
        ];
    }
    
    /**
     * 获取关系信息
     *
     * @param int $relationId 关系ID
     * @return array|null 关系信息，如果不存在则返回null
     */
    public function getRelation(int $relationId): ?array {
        return $this->relations[$relationId] ?? null;
    }
    
    /**
     * 获取数据类型信息
     *
     * @param int $typeId 类型ID
     * @return array|null 类型信息，如果不存在则返回null
     */
    public function getType(int $typeId): ?array {
        return $this->types[$typeId] ?? null;
    }
    
    /**
     * 获取完整表名（包含命名空间/模式）
     *
     * @param int $relationId 关系ID
     * @return string|null 完整表名，如果不存在则返回null
     */
    public function getFullTableName(int $relationId): ?string {
        if (!isset($this->relations[$relationId])) {
            return null;
        }
        
        $relation = $this->relations[$relationId];
        return "{$relation['namespace']}.{$relation['relation_name']}";
    }
    
    /**
     * 获取表的列名列表
     *
     * @param int $relationId 关系ID
     * @return array|null 列名数组，如果不存在则返回null
     */
    public function getColumnNames(int $relationId): ?array {
        if (!isset($this->relations[$relationId])) {
            return null;
        }
        
        $columns = [];
        foreach ($this->relations[$relationId]['columns'] as $column) {
            $columns[] = $column['name'];
        }
        
        return $columns;
    }
    
    /**
     * 获取表的主键列
     *
     * @param int $relationId 关系ID
     * @return array 主键列名数组
     */
    public function getPrimaryKeyColumns(int $relationId): array {
        if (!isset($this->relations[$relationId])) {
            return [];
        }
        
        $keyColumns = [];
        foreach ($this->relations[$relationId]['columns'] as $column) {
            if ($column['is_key']) {
                $keyColumns[] = $column['name'];
            }
        }
        
        return $keyColumns;
    }
    
    /**
     * 将元组数据与列名关联
     *
     * @param int $relationId 关系ID
     * @param array $tupleData 元组数据
     * @return array 关联后的数据，格式为 ['列名' => '值', ...]
     */
    public function mapTupleDataToColumns(int $relationId, array $tupleData): array {
        if (!isset($this->relations[$relationId])) {
            return $tupleData;
        }
        
        $result = [];
        $columns = $this->relations[$relationId]['columns'];
        
        // 处理元组数据可能比列定义少的情况
        $columnCount = min(count($tupleData), count($columns));
        
        for ($i = 0; $i < $columnCount; $i++) {
            if (isset($columns[$i])) {
                $columnName = $columns[$i]['name'];
                $result[$columnName] = $tupleData[$i];
            } else {
                $result["column_{$i}"] = $tupleData[$i];
            }
        }
        
        // 处理元组数据可能比列定义多的情况
        for ($i = $columnCount; $i < count($tupleData); $i++) {
            $result["extra_{$i}"] = $tupleData[$i];
        }
        
        return $result;
    }
    
    /**
     * 清除所有映射数据
     *
     * @return void
     */
    public function clear(): void {
        $this->relations = [];
        $this->types = [];
    }
} 