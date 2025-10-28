# DDL同步处理流程图

## 整体处理流程

```mermaid
flowchart TD
    A[源数据库DDL变更] --> B[监听器捕获DDL事件]
    B --> C[创建DDLChangedEvent]
    C --> D[发送到缓冲执行器]
    D --> E{事件类型判断}

    E -->|DDL事件| F[GeneralBufferActuator]
    E -->|数据事件| G[TableGroupBufferActuator]

    F --> H[parseDDl方法]
    H --> I[DDL解析与转换]
    I --> J[执行目标DDL]
    J --> K[更新字段映射]
    K --> L[刷新缓存配置]
    L --> M[刷新偏移量]

    G --> N[distributeTableGroup方法]
    N --> O[字段映射处理]
    O --> P[数据转换]
    P --> Q[插件处理]
    Q --> R[批量执行同步]
    R --> S[持久化结果]

    M --> T[处理完成]
    S --> T
```

## 详细处理步骤

### 1. 事件捕获阶段

```mermaid
flowchart TD
    A[源数据库DDL变更] --> B[MySQLListener/SqlServerListener]
    B --> C[创建DDLChangedEvent]
    C --> D[设置事件属性]
    D --> E[发送到BufferActuatorRouter]
    E --> F{路由判断}

    F -->|DDL事件| G[GeneralBufferActuator.offer]
    F -->|数据事件| H[TableGroupBufferActuator.offer]

    G --> I[缓冲队列排队]
    H --> I
```

### 2. 缓冲执行器处理阶段

#### 2.1 AbstractBufferActuator.submit() 方法

```mermaid
flowchart TD
    A[定时器触发] --> B[获取队列锁]
    B --> C{队列是否为空}
    C -->|是| D[释放锁返回]
    C -->|否| E[开始批量处理]

    E --> F[初始化计数器]
    F --> G[初始化响应映射]
    G --> H[循环处理队列]

    H --> I[从队列取出请求]
    I --> J[获取分区键]
    J --> K[创建/获取响应对象]
    K --> L[partition合并处理]
    L --> M[计数器递增]

    M --> N{达到批次大小?}
    N -->|是| O[process处理批次]
    N -->|否| P[检查下一个请求]

    P --> Q{skipPartition返回true?}
    Q -->|是| O
    Q -->|否| H

    O --> R[清空映射]
    R --> S[重置计数器]
    S --> H

    H --> T[队列处理完成]
    T --> U[处理剩余数据]
    U --> V[释放资源]
    V --> W[释放锁]
```

#### 2.2 GeneralBufferActuator.pull() 方法

```mermaid
flowchart TD
    A[pull方法调用] --> B[获取Meta配置]
    B --> C{Meta是否存在}
    C -->|否| D[直接返回]
    C -->|是| E[获取Mapping配置]

    E --> F[获取TableGroupPicker]
    F --> G{事件类型判断}

    G -->|DDL| H[调用parseDDl方法]
    G -->|SCAN| I[全量扫描处理]
    G -->|ROW| J[增量数据处理]

    H --> K[DDL解析与执行]
    I --> L[distributeTableGroup false]
    J --> M[distributeTableGroup true]

    K --> N[处理完成]
    L --> N
    M --> N
```

### 3. DDL处理详细流程 (parseDDl方法)

```mermaid
flowchart TD
    A[parseDDl方法开始] --> B[获取源/目标连接器配置]
    B --> C{是否启用DDL同步}
    C -->|否| D[跳过DDL执行]
    C -->|是| E[DDL解析]

    E --> F[生成目标DDLConfig]
    F --> G[执行目标DDL]
    G --> H{执行结果判断}

    H -->|成功| I[持久化增量事件]
    H -->|失败| J[记录错误日志]

    I --> K[更新表结构信息]
    J --> K

    K --> L[更新字段映射关系]
    L --> M[更新执行命令]
    M --> N[持久化表组配置]
    N --> O[刷新偏移量]
    O --> P[处理完成]
```

### 4. 数据同步详细流程 (distributeTableGroup方法)

```mermaid
flowchart TD
    A[distributeTableGroup开始] --> B[字段映射处理]
    B --> C[启用模式解析器?]
    C -->|是| D[使用SchemaResolver]
    C -->|否| E[直接映射]

    D --> F[数据类型转换]
    E --> F

    F --> G[参数转换处理]
    G --> H[插件预处理]
    H --> I[批量执行同步]
    I --> J{执行结果判断}

    J -->|成功| K[持久化同步结果]
    J -->|失败| L[记录错误信息]

    K --> M[插件后处理]
    L --> M
    M --> N[处理完成]
```

## 关键时序控制点

### 1. 分区跳过机制 (skipPartition)

```mermaid
flowchart TD
    A[skipPartition调用] --> B{事件类型是否相同}
    B -->|否| C[返回true - 跳过]
    B -->|是| D{当前响应是否为DDL}

    D -->|是| E[返回true - 跳过]
    D -->|否| F[返回false - 继续]

    C --> G[中断当前批次]
    E --> G
    F --> H[继续合并处理]
```

### 2. 批次处理控制

```mermaid
flowchart TD
    A[批次处理开始] --> B{队列中下一个请求}
    B -->|存在| C[检查skipPartition]
    B -->|不存在| D[处理当前批次]

    C --> E{skipPartition返回true?}
    E -->|是| F[立即中断处理当前批次]
    E -->|否| G[继续合并到当前批次]

    F --> D
    G --> H[继续处理下一个请求]
    H --> B

    D --> I[清空映射重置计数器]
    I --> J[处理完成]
```

## 异常处理流程

### 1. DDL执行异常

```mermaid
flowchart TD
    A[DDL执行异常] --> B[捕获异常]
    B --> C[记录错误日志]
    C --> D[跳过DDL执行步骤]
    D --> E[继续后续处理]
    E --> F[更新字段映射,可能失败]
    F --> G[处理完成]
```

### 2. 数据同步异常

```mermaid
flowchart TD
    A[数据同步异常] --> B[捕获异常]
    B --> C[记录错误信息]
    C --> D[标记失败数据]
    D --> E[继续处理其他数据]
    E --> F[持久化部分结果]
    F --> G[处理完成]
```

## 性能优化点

1. **批量处理**: 通过 `BufferPullCount` 控制批次大小
2. **分区合并**: 相同分区的请求合并处理
3. **异步执行**: 使用线程池异步执行数据同步
4. **缓存机制**: 表结构信息缓存减少数据库查询
5. **流式处理**: 支持大文件流式处理

这个流程图完整展示了从缓存拉取到执行目标库SQL的整个处理过程，包括DDL同步和数据同步的协调机制。