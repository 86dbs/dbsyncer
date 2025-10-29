# 已有设计

## 全量同步 vs 增量同步

全量同步（Full Synchronization）：
- FullPuller 全量同步入口
- TableGroup 保存 cursor 信息
- BufferActuator 写入数据（不使用队列）

增量同步（Incremental Synchronization）：
- 通过Listener监听数据变更
- BufferActuatorRouter.execute() 发送 ChangedEvent来表示数据变更事件
- 使用BufferActuatorRouter.offer 将数据写入队列，使用队列的原因：打包零散的数据变化，提高写入的性能。
- AbstractBufferActuator 定时器从队列中拉取数据
- 使用 Meta 保存源的偏移量，每3s写入偏移量信息

## 核心数据

Mapping 定义了"做什么"和"怎么做"，即同步任务的配置和参数。
Meta 记录了"做得怎么样"，即同步任务的执行状态和进度。对应印个Task, 两者的ID相同


## 核心类

BufferActuator
- TableGroupBufferActuator 处理字段映射，每个表对应一个实例，具有更好的性能（独立线程池），但有数量限制，每个驱动器 max-buffer-actuator-size 个
- GeneralBufferActuator 在没有专用 TableGroupBufferActuator 的情况下发挥作用, 由 BufferActuatorRouter 决定。处理数据变更和DDL变更，并触发事件回调。
- StorageBufferActuator 处理数据持久化任务
- AbstractBufferActuator 有定时器，每 300ms 触发一次，从缓存中拉取数据。有重入锁，保证同时只有一个任务在执行。

BufferActuatorRouter 只处理增量同步模式

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

## 异构数据库字段映射

- DataTypeEnum 提供了统一的类型
- AbstractDataType<T> 实现类型转换的核心逻辑：
- SchemaResolver 接口：定义了类型转换的核心方
- 数据转换核心类 - Picker

过程如下：
- 数据读取阶段: 从源数据库读取原始数据，在 GeneralBufferActuator 中触发
- 字段映射阶段: 通过Picker.exchange()进行字段映射和值传递
- 类型标准化阶段: 通过SchemaResolver.merge()将源数据转换为标准类型
- 类型转换阶段: 通过SchemaResolver.convert()将标准类型转换为目标数据库类型
- 数据写入阶段: 将转换后的数据写入目标数据库

源数据库数据 → [Picker.merge] → Java标准类型 → [AbstractConnector.convert] → 目标数据库数据
↓              ↓                    ↓                    ↓
SQL Server     sourceResolver     标准化数据         targetResolver
int identity   .merge()           (Integer)         .convert()
↓              ↓                    ↓                    ↓
原始数据      转换为标准类型        中间状态

## DDL 同步

[DDL 同步流程](ddl-processing-flow.md)

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