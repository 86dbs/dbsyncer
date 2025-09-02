发布日志

## 2.3.0 (未发布) 

全量+增量
字段全选

## 2.2.0 (未发布)

- 将 kafka 的 topic 移植到 meta 中，并简化 kafka 的配置。
- bug fixed: max 线程数小于 core 线程数，线程池异常问题。（OK）

## 2.1.1
性能优化：RefreshOffsetEvent，FullRefreshEvent 使用回调机制替代 spring 事件机制

## 2.1.0 2025年8月5日

- bug fixed: 数据同步到 kafka ，kafka接收不到消息
- 支持 bit 数据类型
- ui: when target table is null then use source table name
- ui: when target field is null then use source field name
- kafka use one topic to receive all tables
- kafka 输出格式兼容 DataPipeline
