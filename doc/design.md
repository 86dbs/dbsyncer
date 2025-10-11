# 已有设计

## 全量同步 vs 增量同步

全量同步（Full Synchronization）：
- 使用Task类来跟踪同步进度
- 在FullPuller中创建和管理Task实例
- Task保存了同步过程中的分页信息、表组索引等状态

增量同步（Incremental Synchronization）：
- 使用 ChangedEvent来表示数据变更事件
- 使用 RefreshOffsetEvent 更新源的偏移量
- 通过Listener监听数据变更
- 使用BufferActuatorRouter和相关执行器处理变更事
- 每3s写入snapshot信息

## 核心数据

Mapping 定义了"做什么"和"怎么做"，即同步任务的配置和参数。
Meta 记录了"做得怎么样"，即同步任务的执行状态和进度。对应印个Task, 两者的ID相同


## 核心类

BufferActuator
- TableGroupBufferActuator 处理字段映射，每个表对应一个实例，具有更好的性能（独立线程池），但有数量限制，每个驱动器 max-buffer-actuator-size 个
- GeneralBufferActuator 在没有专用 TableGroupBufferActuator 的情况下发挥作用, 由 BufferActuatorRouter 决定
- StorageBufferActuator 处理数据持久化任务

BufferActuatorRouter 只处理增量同步模式
GeneralBufferActuator：处理数据变更和DDL变更，并触发 RefreshOffsetEvent 事件

Listener：
  - 依据连接类型和监听类型（日志|定时）创建监听器
  - 可注册多个 watcher

## 连接器

**DQL连接器（Data Query Language）**：
用途：专门用于执行自定义SQL查询语句
特点：支持用户自定义复杂的SQL查询，不局限于单表操作
适用场景：需要执行复杂查询、多表关联、聚合统计等场景

**非DQL连接器**：
用途：用于标准的数据库表操作或特定数据源操作
特点：基于表结构进行标准的增删改查操作
适用场景：常规的表同步、数据迁移等场景


## 状态

MetaEnum：在 Meta 中记录同步任务的整体状态
- READY，RUNNING，STOPPING

StateEnum：在 Task 中记录具体执行任务的状态
- RUNNING，STOP

## 包依赖关系

dbsyncer-common (最底层)
    ↑
dbsyncer-sdk (依赖common)
    ↑
dbsyncer-connector-* (各连接器依赖sdk)
    ↑
dbsyncer-connector-base (聚合所有连接器)
    ↑
dbsyncer-storage (依赖connector-base)
    ↑
dbsyncer-plugin (依赖sdk + connector)
    ↑
dbsyncer-parser (依赖plugin + storage)
    ↑
dbsyncer-manager (依赖parser)
    ↑
dbsyncer-biz (依赖manager)
    ↑
dbsyncer-web (依赖biz，最顶层)


## 局限性

### mapping.params

因当前架构无法强 mapping.params 直接传递给 writer, 所以将 mapping.params 运行时放到了 context.commands 中。