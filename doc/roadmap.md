# 待解决的问题或缺陷

## 优先级高的问题

- 过滤条件简化：过滤条件直接输入表达式

## 问题池

- 增量定时-移除 AbstractDatabaseConnector.reader
- DLParserImpl  68 需要优化
- MetaInfo 是否多余？
- 使用 sql 模板移除 MySQLStorageService 中零散的 sql 拼接方式。 
- AbstractDatabaseConnector.filterColumn 这个应该在编辑时处理，而不是在运行时处理
- 增量-定时 重构
- 优化：统计，UI 显示输入成功数，失败数，阶段
- 状态错乱：在全量同步大表时，同步过程中出现“进行中”变为“未开始”状态，但数据仍然同步问题，不影响二次开启。
- 队列溢出问题：删除队列，使用 kafka 作为中间数据源
- 系统配置问题：在数据库里会形成多份“系统配置”，应该为一份
- 优化：监控界面增加任务结束的记录


