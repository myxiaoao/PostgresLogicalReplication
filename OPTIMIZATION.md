# 项目优化说明

## 优化内容

本次优化主要针对 PostgreSQL 逻辑复制解析器项目进行了以下改进：

1. **删除已废弃的二进制解析相关代码**
   - 移除了 `LogicalReplicationParser` 类中的 `parse()` 方法
   - 只保留 `parseWal2json()` 方法作为唯一的解析方式

2. **优化代码结构**
   - 简化了 `PostgresLogicalReplication` 类中的方法和注释
   - 删除了不必要的调试代码和过时的注释
   - 确保所有代码都符合 PHP 8.0 的语法和特性

3. **优化示例代码**
   - 更新了 `example.php` 示例文件，删除了与二进制解析相关的代码
   - 优化了 `business_example.php` 中的业务逻辑处理
   - 简化了 `recreate_slot.php` 文件的注释

4. **更新文档**
   - 更新了 README.md 文件，确保文档内容与代码一致
   - 删除了关于二进制解析的过时说明
   - 优化了文档结构和内容

## 技术规范

- PHP 最低版本：8.0
- 解析方式：仅支持 wal2json
- 依赖：
  - ext-pdo
  - ext-pdo_pgsql
  - monolog/monolog: ^2.0

## 使用说明

项目现在只支持使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出。确保您的 PostgreSQL 服务器已安装并配置了 wal2json 插件。

详细的使用说明请参考 README.md 文件。 