# 待解决的问题或缺陷

## 优先级高的问题

- 性能优化：解决缓存定时拉取在大规模应用中的锁竞争和线程切换开销问题。
- 稳定性优化：增加增量同步异常的中断处理机制，避免缓存撑爆风险
- 过滤条件简化：过滤条件直接输入表达式
- 功能新增：UI 增加同步阶段的显式。
- 优化：提交任务时对 fieldMapping 进行检测，如空不允许提交。
- bug：失败数对应的日志缺失
- bug：UI 自动刷新导致操作列表消失问题

## 问题池

- 增量-定时 重构
  - 增量定时-移除 AbstractDatabaseConnector.reader
- DLParserImpl  68 需要优化
- MetaInfo 是否多余？
- 使用 sql 模板移除 MySQLStorageService 中零散的 sql 拼接方式。 
- AbstractDatabaseConnector.filterColumn 这个应该在编辑时处理，而不是在运行时处理
- 队列溢出问题：删除队列，使用 kafka 作为中间数据源
- 系统配置问题：在数据库里会形成多份“系统配置”，应该为一份
- 优化：监控界面增加任务结束的记录


