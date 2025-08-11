# 设计

## 全量同步 vs 增量同步

全量同步（Full Synchronization）：
- 使用Task类来跟踪同步进度
- 在FullPuller中创建和管理Task实例
- Task保存了同步过程中的分页信息、表组索引等状态

增量同步（Incremental Synchronization）：
- 使用ChangedEvent来表示数据变更事件
- 通过Listener监听数据变更
- 使用BufferActuatorRouter和相关执行器处理变更事

## 核心数据

Mapping 定义了"做什么"和"怎么做"，即同步任务的配置和参数。
Meta 记录了"做得怎么样"，即同步任务的执行状态和进度。对应印个Task, 两者的ID相同


## 核心类

BufferActuator
- TableGroupBufferActuator 处理字段映射
- GeneralBufferActuator 在没有专用 TableGroupBufferActuator 的情况下发挥作用, 由 BufferActuatorRouter 决定
- StorageBufferActuator 处理数据持久化任务

BufferActuatorRouter 只处理增量同步模式
GeneralBufferActuator：处理数据变更和DDL变更，并触发 RefreshOffsetEvent 事件
