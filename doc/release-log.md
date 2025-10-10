发布日志

## 2.5.0

- 性能优化：mysql, sql server 使用流式读取数据，大表性能提升数倍，支持断点续传。
- 性能优化：支持多表并发处理。
- 结构优化：将 DQL 类连接器重构，简化代码。
- 结构优化：使用 sql template 技术 替代 sql builder 技术，简化 sql 生成的层次结构。(OK)；
- 结构优化：用 Meta 替代 Task 类，解决 Task 在多线程环境下导致的任务有时无法关停问题。
- 优化：meta 和 tableGroup 的名字更具有可读性和意义。
- 优化：使用左右引号而非单一引号，解决 SQL server 引入的双重复杂度。
- 优化: tableGroup 不需要持久化 sql（OK）
- bug fixed: 编辑导致状态重置问题。(OK)
- bug fixed：重置计数为历史数量问题。(OK)
- bug fixed: 复制的任务，数据总数没有计算问题。（OK）

## 2.4.0

- 新功能：全量+增量混合模式实现(Ok)
- 新功能：mapping 的扩展参数可下拉选择(OK)
- 新功能：为“驱动”提供异常信息的展示。（OK）
- 新功能：增加重置状态（仅适用于混合模式）(ok)
- 新功能：增加了异常状态。（OK）
- 性能优化：提升 mysql 源端解析性能。(OK)
- 优化：取消 closeEvent 事件的使用（OK）
- 优化：移除 AbstractPuller 类（OK）
- 优化: Meta 集成持久化逻辑，以简化维护。（OK）
- 优化：puller 接口 优化。(OK)
- 优化：移除 mete stopping 状态（OK）

## 2.3.1

- 新功能：字段全选既过滤功能
- disable kafka group.id check
- bug fixed: 以 kafka 为目标的新同步任务，配置时无法保存。
- bug fixed: pk missed when add mapping

## 2.2.0

- bug fixed: max 线程数小于 core 线程数，线程池异常问题。
- 驱动：新增扩展参数编辑能力。
- 扩展参数：可覆盖 kafka 的缺省 topic 设置。
- 移除 kafka 的字段定义设置。

## 2.1.1
性能优化：RefreshOffsetEvent，FullRefreshEvent 使用回调机制替代 spring 事件机制

## 2.1.0 2025年8月5日

- bug fixed: 数据同步到 kafka ，kafka接收不到消息
- 支持 bit 数据类型
- ui: when target table is null then use source table name
- ui: when target field is null then use source field name
- kafka use one topic to receive all tables
- kafka 输出格式兼容 DataPipeline
