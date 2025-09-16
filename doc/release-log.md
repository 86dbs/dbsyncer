发布日志

## 2.4.0 (未发布) 

- 新功能：全量+增量混摸模式实现(Ok)
- 新功能：mapping 的扩展参数可下拉选择(OK)
- 新功能：为“驱动”提供异常信息的展示。（OK）
- 新功能：增加暂停状态（仅适用于混合模式）。
- 优化：对异常处理机制进行了优化，状态中增加了异常状态。（OK）
- 优化：取消 closeEvent 事件的使用（OK）
- 优化：移除 AbstractPuller 类（OK）
- 优化：提升 sql 解析性能。(OK)
- 优化: Meta 集成持久化逻辑，以简化维护。（OK）
- 优化：puller 接口 优化。(OK)
- 性能优化：Mysql 解析弃用线程模式。（OK）
- 优化：移除 mete stopping 状态，多余（OK）

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
