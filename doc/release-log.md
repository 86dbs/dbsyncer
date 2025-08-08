发布日志

## 2.2.0

- 架构调整，每个任务独享 puller，提升性能和可维护性。
- 
- 全量加增量

## 2.1.0 2025年8月5日

- bug fixed: 数据同步到 kafka ，kafka接收不到消息
- 支持 bit 数据类型
- ui: when target table is null then use source table name
- ui: when target field is null then use source field name
- kafka use one topic to receive all tables
- kafka 输出格式兼容 DataPipeline
