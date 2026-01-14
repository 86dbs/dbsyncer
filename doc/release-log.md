# 发布日志

## 2.13

- 重大缺陷：解决反复启动丢数据问题。
- 废弃配置列表：dbsyncer.parser.table.group.*
- 

## 2.12

- 新功能：可看每个表的同步状态。
- 新功能：实现了数据源列表的搜索功能，可以根据数据源名称，类型，ip进行模糊查询
- 重构：快照保存机制。
- 缺陷：修复刷新表按钮无法同步数据源表最新的数据的 bug
- 缺陷：修复各个界面的返回按钮只能返回到首页的bug
- 界面优化：简化任务界面的卡片及列表布局，每行数量从两个增加到四个
- 界面优化：将复制，删除，重置，日志按钮移动到任务详细页面（原来的编辑页面）
- 在任务列表页面新增三个下拉表单用于检索任务，
- 其它优化


## 2.11.4

- 新功能：任务一旦开始，编辑功能受限。（OK）
- 优化：前端回退功能优化。（OK）
- 缺陷：更改快照保存机制，避免丢失数据（OK）
- 缺陷：修正任务编辑界面交替显示问题(OK)
- 缺陷：解决 sql server CT 增量不同步问题(Ok)
- 缺陷：修复 mysql 增量同步获取表字段不准问题。
- 严重缺陷：修复增量同步丢数据问题（字段匹配问题）（OK）
- 缺陷：kafka 连接异常，不能创建、编辑任务问题。（OK）
- 缺陷：修复任务中的连接丢失导致任务页面无法加载的问题。(OK)
- 缺陷：修复监控页因 Meta 缺失 Mapping 导致无法加载问题。(OK)
- 优化：新建、编辑 Mapping 时检查连接是否存在。(OK)
- 优化：先创建 Mapping 后创建 Meta。(OK)
- 优化：避免 sql 生成时加载无效连接。(OK)
- 缺陷：修复启动日志各个配置计数不准确问题。（OK）

## 2.10

- 新功能：可以从 binlog 的任意位置开始，
- 优化快照更新机制。
- 缺陷：修复mysql binlog json 字段解析错误

## 2.9.1

- 新功能：任务增加列表方式。
- 新功能：提供任务查询功能。
- 功能调整：数据源独立管理。
- 缺陷：修复 sql server 参数达 2100 个问题。
- 缺陷：修复 sql server CT 模式下无法删除数据问题。
- 缺陷：修复 sql server 同构同步自增主键无法建表问题。
- 性能优化：sql server CT 模式下避免 DML 每次都查主键（getPrimaryKeys）。

## 2.8.0

- 新功能：基于 CT(change tracing) 实现 mssql ddl(DML 驱动)，之所以不用 cdc 主要是 sql server
  端的压力，同时消除表结构变化这种自身缺陷影响同步数据正确性问题.(OK)
- 新功能：对连接器增加“是否支持DDL写入”功能，以忽略 kafka 等非标准数据库的建表检测动作。（OK）
- 缺陷修复：不同线程之间串数据问题。（command, table）(ok)
- 缺陷修复：修复无法重建与 kaka 的表映射关系。（OK）
- 性能优化：使用性能更高的记录数查询机制。(OK)
- 结构优化：字段映射主要参考原表，简化操作过程。（OK）
- 优化：对无主键数据表进行过滤处理，并形成错误日志。(OK)

## 2.7.0

- 新功能：目标表缺失感知与创建。
- 新功能：支持目标表重命名。
- 新功能：Mysql 支持DDL 支持。增加了相应的开关：加列、删列、改列。注意，忽略了缺省值的处理，因为一是每种库缺省值的函数表达非常不同，比如时间戳，较为复杂；二是不影响同步结果。
    - 优化：重构中间标准化类型, 并对标准类型进行了扩展(XML,JSON,ENUM,SET,TEXT,UNSIGNED,unicode,UUID,BLOB,geometry),移除
      ARRAY 类型。
- 结构优化：大幅度改善不同数据库的 schema 解析工作的抽象，提供其可维护能力。
- 优化：停止同时使用两套json框架，移除 fastjson 的使用，给维护减压。
- UE 优化：任务面板布局重构。(OK)

## 2.6.0

- 性能优化：重构同步覆盖，使用 upsert 技术替代现有的非批量、IO繁重的方式。
- 性能优化：大幅度提升sql server 写入慢问题。
- 性能优化: 解决重复加工 sql 指令列表问题。（OK）
- UE 优化：优化分组列表的显示。
- UE 优化：解决上下文弹出菜单因自动刷新而消失的问题。(OK)
- bug fixed: 解决 mysql, mssql 发现的类型转化问题。
- bug fixed: 全部表移除时计数统计不为0问题。(OK)

## 2.5.0

- 新功能：增加 Sql Server 的异构同步输出机制。
- 结构优化：优化字段映射处理机制
- 性能优化：mysql, sql server 使用流式读取数据，大表性能提升数倍，支持断点续传。
- 性能优化：支持多表并发处理。
- 性能优化：减少从缓存读取数据的任务线程的竞争数量
- 性能优化：同步过程错误信息会累加并不断膨胀，对性能、和系统稳定性影响较大。
- 调整：直接使用 JSqlPhase，废弃对 JSqlPhase 的包装（2万行）
- 结构优化：将 DQL 类连接器重构，简化代码。
- 结构优化：使用 sql template 技术 替代 sql builder 技术，简化 sql 生成的层次结构。(OK)；
- 结构优化：用 Meta 替代 Task 类，解决 Task 在多线程环境下导致的任务有时无法关停问题。
- 结构优化：对 TableGroup 和 Mapping 进行了功能扩展，以降低主线程序的复杂度。
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
