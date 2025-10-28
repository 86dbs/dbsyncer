# DDL 同步

## 场景覆盖

数据库类型: mysql, sql server.

变更覆盖：字段增加、删除、修改

## 要点

DDL 同步于数据同步的关系。

## 能力覆盖分析

### 支持的数据库类型
- **MySQL**: 完全支持DDL同步
- **SQL Server**: 完全支持DDL同步  
- **Oracle**: 支持DDL变更捕获，但同步能力待验证
- **PostgreSQL**: 基础DDL支持，同步能力待验证

### 支持的DDL操作类型
基于 `DDLOperationEnum` 枚举定义，支持以下DDL操作：
- **ALTER_MODIFY**: 变更字段属性（类型、长度等）
- **ALTER_ADD**: 新增字段
- **ALTER_CHANGE**: 变更字段名称
- **ALTER_DROP**: 删除字段

### 技术架构

#### 1. DDL变更捕获机制
- **MySQL**: 通过 `MySQLListener` 监听binlog中的DDL事件
- **SQL Server**: 通过 `SqlServerListener` 监听CDC变更日志
- **Oracle**: 通过 `OracleListener` 监听redo日志中的DDL事件

#### 2. DDL解析引擎
- 使用 `DDLParserImpl` 解析源数据库的DDL语句
- 基于JSQLParser库解析SQL语法树
- 支持表名替换（源表名→目标表名）
- 解析DDL操作类型并提取变更字段信息

#### 3. 字段映射同步
- 自动更新 `TableGroup` 中的字段映射关系
- 支持字段增删改操作对映射关系的实时同步
- 维护源表和目标表字段结构的一致性

#### 4. 目标DDL执行
- 通过 `AbstractDatabaseConnector.writerDDL()` 执行目标DDL
- 添加 `/*dbs*/` 标识防止双向同步死循环
- 支持事务性DDL执行和错误处理

## 处理流程

### 1. DDL变更捕获流程
```
源数据库DDL变更 → 监听器捕获DDL事件 → 创建DDLChangedEvent → 发送到缓冲器
```

### 2. DDL解析与转换流程  
```
DDLChangedEvent → DDLParser解析SQL → 表名替换 → 操作类型识别 → 生成DDLConfig
```

### 3. 字段映射更新流程
```
DDLConfig → 根据操作类型更新字段映射 → 同步源表和目标表结构
```

### 4. 目标DDL执行流程
```
DDLConfig → 目标连接器执行DDL → 添加防循环标识 → 返回执行结果
```

## 配置要求

### 启用DDL同步
- 在监听器配置中设置 `enableDDL = true`
- 仅支持同类型数据库间的DDL同步（MySQL→MySQL，SQL Server→SQL Server）
- 需要源数据库开启相应的日志功能（binlog、CDC等）

### 限制条件
- 暂不支持跨数据库类型的DDL同步
- 复杂的DDL操作（如索引变更、约束变更）支持有限
- 需要确保源和目标数据库的权限配置正确

## 技术实现细节

### DDL解析策略
项目实现了四种DDL解析策略：
- `AddStrategy`: 处理新增字段操作
- `DropStrategy`: 处理删除字段操作  
- `ModifyStrategy`: 处理字段属性修改
- `ChangeStrategy`: 处理字段重命名

### 防循环机制
通过在DDL语句前添加 `/*dbs*/` 标识，防止在双向同步场景下产生死循环。

### 错误处理
DDL执行失败时会记录详细错误信息，但不会中断数据同步流程，确保数据同步的稳定性。

## DDL同步与数据同步的协调机制

### 1. 事件类型识别与路由

DBSyncer通过 `ChangedEventTypeEnum` 区分不同类型的变更事件：
- **DDL**: 表结构变更事件
- **ROW**: 数据行变更事件  
- **SCAN**: 全量扫描事件

在 `BufferActuatorRouter` 中根据事件类型进行路由：
```java
if (ChangedEventTypeEnum.isDDL(event.getType())) {
    WriterRequest request = new WriterRequest(event);
    offer(generalBufferActuator, event);
}
```

### 2. 执行器协调机制

#### 2.1 缓冲执行器架构
项目采用三级缓冲执行器架构：

1. **TableGroupBufferActuator**: 表级别专用执行器（高性能）
   - 每个表对应一个独立实例
   - 具有独立的线程池
   - 数量受 `max-buffer-actuator-size` 限制

2. **GeneralBufferActuator**: 通用执行器（协调中心）
   - 处理数据变更和DDL变更
   - 单线程消费，多线程批量写
   - 负责DDL和数据同步的协调

3. **StorageBufferActuator**: 存储执行器
   - 处理数据持久化任务

#### 2.2 DDL处理的原子性保证

在 `GeneralBufferActuator.skipPartition()` 方法中确保DDL处理的原子性：
```java
// 跳过表结构修改事件（保证表结构修改原子性）
return !StringUtil.equals(nextRequest.getEvent(), response.getEvent()) 
       || ChangedEventTypeEnum.isDDL(response.getTypeEnum());
```

**关键机制**: DDL事件不会被合并到批量处理中，确保表结构变更的原子性执行。

### 3. 处理流程协调

#### 3.1 DDL事件处理流程
```
DDL事件 → GeneralBufferActuator → parseDDl()方法 → 执行目标DDL → 更新字段映射 → 刷新缓存
```

#### 3.2 数据事件处理流程  
```
数据事件 → TableGroupBufferActuator/GeneralBufferActuator → distributeTableGroup() → 数据同步
```

#### 3.3 协调关键点

1. **事件顺序保证**: 通过缓冲队列保证DDL和数据变更的顺序性
2. **字段映射同步**: DDL执行后立即更新字段映射关系
3. **缓存一致性**: DDL变更后刷新表结构缓存
4. **偏移量管理**: 统一的偏移量刷新机制

### 4. 字段映射的动态更新

DDL同步的核心协调机制体现在字段映射的动态更新：

#### 4.1 DDL执行后的映射更新
在 `parseDDl()` 方法中完成：
```java
// 3.更新表属性字段
MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(...);
MetaInfo targetMetaInfo = connectorFactory.getMetaInfo(...);
tableGroup.getSourceTable().setColumn(sourceMetaInfo.getColumn());
tableGroup.getTargetTable().setColumn(targetMetaInfo.getColumn());

// 4.更新表字段映射关系
ddlParser.refreshFiledMappings(tableGroup, targetDDLConfig);

// 5.更新执行命令
tableGroup.initCommand(mapping, connectorFactory);
```

#### 4.2 映射更新策略
根据不同的DDL操作类型：
- **ALTER_ADD**: 新增字段映射
- **ALTER_DROP**: 删除字段映射  
- **ALTER_MODIFY**: 更新字段属性映射
- **ALTER_CHANGE**: 重命名字段映射

### 5. 性能与一致性平衡

#### 5.1 性能优化
- DDL事件单独处理，避免影响数据同步性能
- 字段映射缓存机制减少数据库元数据查询
- 批量处理机制提高数据同步效率

#### 5.2 一致性保证
- DDL操作的原子性执行
- 字段映射的实时同步
- 缓存数据的一致性更新
- 事务性DDL执行

### 6. 异常处理协调

#### 6.1 DDL执行失败处理
- DDL执行错误记录日志但不中断流程
- 字段映射更新失败回滚操作
- 数据同步继续使用旧的字段映射

#### 6.2 数据同步异常处理
- DDL变更期间的数据同步使用最新字段映射
- 字段不存在时的优雅降级处理
- 自动重试机制确保最终一致性

### 7. 配置驱动的协调

协调行为通过配置参数控制：
- `enableDDL`: 是否启用DDL同步
- `max-buffer-actuator-size`: 表执行器上限
- `enableSchemaResolver`: 是否启用模式解析器

这种协调机制确保了DDL同步和数据同步的高效、可靠协同工作，既保证了数据一致性，又提供了良好的性能表现。