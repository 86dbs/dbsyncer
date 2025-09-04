发布日志

## 2.4.0 (未发布) 

全量+增量
字段全选

## 2.3.0

- 新功能：字段全选既过滤功能
- disable kafka group.id check
- bug fixed: 以 kafka 为目标的新同步任务，配置时无法保存。

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
