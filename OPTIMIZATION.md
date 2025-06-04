# 项目优化说明

## 优化内容

本次优化针对 PostgreSQL 逻辑复制解析器项目进行了以下改进：

1. **代码结构优化**
   - 重新组织了 `PostgresLogicalReplication` 类的方法顺序，按照公共方法和私有方法分组
   - 提取了重复代码到独立的辅助方法中，如 `createRegularConnection()` 和 `createReplicationConnection()`
   - 将复杂逻辑拆分为更小的函数，如 `validateReplicationSlot()` 和 `buildReplicationQuery()`

2. **性能优化**
   - 减少了重复的连接字符串构建
   - 优化了日志记录，减少不必要的调试输出
   - 改进了错误处理流程，提高了系统稳定性

3. **文档完善**
   - 为所有方法添加了 PHPDoc 注释，包括参数和返回值类型
   - 统一使用中文错误消息，提高可读性
   - 更新了 README.md 文件，使其与当前代码实现保持一致

4. **示例代码优化**
   - 改进了 `example.php` 示例文件，展示更完整的变更数据处理方式
   - 添加了更详细的注释，帮助用户理解代码流程
   - 优化了错误处理和资源清理

5. **类型安全**
   - 添加了更严格的类型声明，包括参数类型和返回值类型
   - 使用 PHP 8.0 特性，如联合类型和空值合并运算符
   - 明确捕获 `JsonException` 异常，提高 JSON 解析的安全性

## 技术规范

- PHP 最低版本：8.0
- 解析方式：仅支持 wal2json
- 依赖：
  - ext-pdo
  - ext-pdo_pgsql
  - ext-pgsql
  - ext-pcntl（可选）
  - monolog/monolog: ^2.0

## 使用说明

项目使用 wal2json 插件解析 PostgreSQL 的逻辑复制输出。确保您的 PostgreSQL 服务器已安装并配置了 wal2json 插件。

主要类 `PostgresLogicalReplication` 提供以下功能：
- 自动创建和管理复制槽和发布
- 监听数据库变更并通过回调函数处理
- 自动重连和错误恢复
- 可配置的心跳机制
- 信号处理（优雅关闭）

详细的使用说明请参考 README.md 文件。 