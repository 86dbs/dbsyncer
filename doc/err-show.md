# 驱动运行异常提示功能设计文档

## 需求背景

在DBSyncer系统中，用户需要能够直观地了解驱动的运行状态，特别是当驱动出现异常时，需要有明确的视觉提示来帮助用户快速识别和处理问题。

## 需求分析

目前系统只显示了连接器的连接状态，但没有显示驱动本身的运行异常。需要增加驱动运行异常的提示功能，包括：

1. 驱动运行时异常的可视化提示
2. 明确的错误信息展示
3. 与现有UI风格保持一致

## 设计方案

根据业务架构角度的考虑，我们将异常状态作为[MetaEnum](file:///e:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/enums/MetaEnum.java)中的一个枚举值来处理，并在Meta对象上补充异常信息属性。

### 1. 扩展Meta类

在[Meta](file:///e:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Meta.java)类中添加异常信息属性：

```java
public class Meta extends ConfigModel {
    // ... 现有字段 ...
    
    // 驱动异常信息
    private String errorMessage = "";
    
    // ... 现有代码 ...
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    private void init(){
        this.state = MetaEnum.READY.getCode();
        this.total = new AtomicLong(0);
        this.success = new AtomicLong(0);
        this.fail = new AtomicLong(0);
        this.snapshot = new HashMap<>();
        this.beginTime = 0L;
        this.endTime = 0L;
        // 初始化异常信息
        this.errorMessage = "";
    }
}
```

### 2. 扩展MetaEnum枚举

在[MetaEnum](file:///e:/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/enums/MetaEnum.java)中添加异常状态枚举值：

```java
/**
 * 驱动状态枚举
 */
public enum MetaEnum {

    /**
     * 未运行
     */
    READY(0, "未运行"),
    /**
     * 运行中
     */
    RUNNING(1, "运行中"),
    /**
     * 停止中
     */
    STOPPING(2, "停止中"),
    /**
     * 异常
     */
    ERROR(3, "异常");

    private final int code;
    private final String message;

    MetaEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static boolean isRunning(int state) {
        return RUNNING.getCode() == state || STOPPING.getCode() == state;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
```

### 3. 在驱动状态管理中增加异常处理

根据驱动的生命周期和状态管理机制，我们需要在关键节点处理异常：

1. **启动异常**：当驱动启动时发生异常
2. **运行异常**：当驱动在运行过程中发生异常
3. **停止异常**：当驱动停止时发生异常

```java
// 在ManagerFactory.java中增加异常处理逻辑
public void start(Mapping mapping) {
    Puller puller = getPuller(mapping);

    // 标记运行中
    changeMetaState(mapping.getMetaId(), MetaEnum.RUNNING);

    try {
        puller.start(mapping);
    } catch (Exception e) {
        // 记录异常状态和异常信息到Meta对象
        recordMappingError(mapping.getMetaId(), e.getMessage());
        throw new ManagerException(e.getMessage());
    }
}

// 在IncrementPuller中增加运行时异常处理
public void start(Mapping mapping) {
    Thread worker = new Thread(() -> {
        try {
            // ... 现有逻辑 ...
        } catch (Exception e) {
            // 记录运行时异常状态和异常信息
            recordMappingError(metaId, e.getMessage());
            close(metaId);
            logService.log(LogType.TableGroupLog.INCREMENT_FAILED, e.getMessage());
            logger.error("运行异常，结束增量同步：{}", metaId, e);
        }
    });
    worker.start();
}

// 在FullPuller中增加运行时异常处理
public void start(Mapping mapping) {
    Thread worker = new Thread(() -> {
        // ... 现有逻辑 ...
        try {
            // ... 现有逻辑 ...
        } catch (Exception e) {
            // 记录运行时异常状态和异常信息
            recordMappingError(metaId, e.getMessage());
            logger.error(e.getMessage(), e);
            logService.log(LogType.SystemLog.ERROR, e.getMessage());
        } finally {
            // ... 现有逻辑 ...
        }
    });
    worker.start();
}

private void recordMappingError(String metaId, String errorMessage) {
    Meta meta = profileComponent.getMeta(metaId);
    if (meta != null) {
        meta.setErrorMessage(errorMessage);
        changeMetaState(metaId, MetaEnum.ERROR);
    }
}

// 在MappingServiceImpl中增加清除异常状态的逻辑
public String start(String id) {
    Mapping mapping = assertMappingExist(id);
    final String metaId = mapping.getMetaId();
    // 如果已经完成了，重置状态
    clearMetaIfFinished(metaId);

    synchronized (LOCK) {
        assertRunning(metaId);
        
        // 启动前清除异常状态和异常信息，恢复到就绪状态
        clearMappingError(metaId);

        // 启动
        managerFactory.start(mapping);

        log(LogType.MappingLog.RUNNING, mapping);
    }
    return "驱动启动成功";
}

private void clearMappingError(String metaId) {
    Meta meta = profileComponent.getMeta(metaId);
    if (meta != null) {
        meta.setErrorMessage("");
        changeMetaState(metaId, MetaEnum.READY);
    }
}
```

### 4. 更新前端页面

在[index.html](file:///e%3A/github/dbsyncer/dbsyncer-web/src/main/resources/public/index/index.html)中增加驱动异常提示显示，并以tooltip形式展示异常信息：

```html
<!-- 在驱动信息显示区域增加异常提示 -->
<div class="row">
    <!--左边驱动信息 -->
    <div class="col-md-5">
        <div class="mapping_well">
            <div class="col-md-4">
                <img draggable="false" th:src="@{'/img/'+ ${m?.sourceConnector?.config?.connectorType} + '.png'}">
            </div>
            <div class="col-md-7 dbsyncer_over_hidden">
                <span th:text="${m?.sourceConnector?.name}" th:title="${m?.sourceConnector?.name}"></span>
            </div>
            <div class="col-md-1"></div>
        </div>
        <span th:if="${m?.sourceConnector?.running}" th:title="连接正常" class="well-sign-left"><i class="fa fa-2x fa-circle well-sign-green"></i></span>
        <span th:unless="${m?.sourceConnector?.running}" th:title="连接异常" class="well-sign-left"><i class="fa fa-2x fa-times-circle-o well-sign-red"></i></span>
    </div>

    <!--中间图标 -->
    <div class="col-md-2">
        <div class="line">
            <span th:if="${m?.meta?.state eq 1}" class="running-through-rate well-sign-green">✔</span>
            <span th:if="${m?.meta?.state eq 0}" class="running-state label label-info">未运行</span>
            <span th:if="${m?.meta?.state eq 1}" class="running-state label label-success">运行中</span>
            <span th:if="${m?.meta?.state eq 2}" class="running-state label label-warning">停止中</span>
            <span th:if="${m?.meta?.state eq 3}" class="running-state label label-danger">异常</span>
            <!-- 驱动异常提示，使用tooltip显示异常信息 -->
            <span th:if="${m?.meta?.state eq 3}" th:title="${m?.meta?.errorMessage}" class="mapping-error-sign" data-toggle="tooltip" data-placement="top"><i class="fa fa-exclamation-triangle"></i></span>
        </div>
    </div>

    <!-- 右边驱动信息 -->
    <div class="col-md-5">
        <div class="mapping_well">
            <div class="col-md-4">
                <img draggable="false" th:src="@{'/img/'+ ${m?.targetConnector?.config?.connectorType} + '.png'}">
            </div>
            <div class="col-md-7 dbsyncer_over_hidden">
                <span th:text="${m?.targetConnector?.name}" th:title="${m?.targetConnector?.name}"></span>
            </div>
            <div class="col-md-1"></div>
            <span th:if="${m?.targetConnector?.running}" th:title="连接正常" class="well-sign-right"><i class="fa fa-2x fa-circle well-sign-green"></i></span>
            <span th:unless="${m?.targetConnector?.running}" th:title="连接异常" class="well-sign-right"><i class="fa fa-2x fa-times-circle-o well-sign-red"></i></span>
        </div>
    </div>
</div>
```

### 5. 添加CSS样式

在[index.css](file:///e%3A/github/dbsyncer/dbsyncer-web/src/main/resources/static/css/index/index.css)中添加驱动异常提示样式：

```css
/* 驱动异常提示样式 */
.mappingList .line .mapping-error-sign {
    position: absolute;
    left: calc(50% + 20px);
    top: 13px;
    font-size: 26px;
    color: #ff0000;
    animation: blink 1s infinite;
    cursor: pointer;
}

/* 闪烁动画 */
@keyframes blink {
    0% { opacity: 1; }
    50% { opacity: 0.3; }
    100% { opacity: 1; }
}
```

## 实现步骤

1. 修改[Meta](file:///e%3A/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Meta.java)类，添加[errorMessage](file:///e%3A/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Meta.java#L107-L107)属性及相关getter/setter方法
2. 修改[MetaEnum](file:///e%3A/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/enums/MetaEnum.java)枚举，添加ERROR枚举值
3. 修改[ManagerFactory](file:///e%3A/github/dbsyncer/dbsyncer-manager/src/main/java/org/dbsyncer/manager/ManagerFactory.java#L30-L30)、[IncrementPuller](file:///e%3A/github/dbsyncer/dbsyncer-manager/src/main/java/org/dbsyncer/manager/impl/IncrementPuller.java#L32-L32)和[FullPuller](file:///e%3A/github/dbsyncer/dbsyncer-manager/src/main/java/org/dbsyncer/manager/impl/FullPuller.java#L39-L170)，增加异常处理逻辑，当驱动出现异常时更新[Meta](file:///e%3A/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Meta.java)对象状态为ERROR并记录异常信息
4. 修改[MappingServiceImpl](file:///e%3A/github/dbsyncer/dbsyncer-biz/src/main/java/org/dbsyncer/biz/impl/MappingServiceImpl.java#L43-L43)，在启动驱动时清除之前的异常状态和异常信息
5. 更新[index.html](file:///e%3A/github/dbsyncer/dbsyncer-web/src/main/resources/public/index/index.html)页面，添加异常状态显示和tooltip提示
6. 在[index.css](file:///e%3A/github/dbsyncer/dbsyncer-web/src/main/resources/static/css/index/index.css)中添加相关样式
7. 测试功能并验证效果

## 业务规则考虑

1. **异常驱动模式设计**：遵循用户偏好的异常驱动模式设计，强调零性能开销，在正常情况下不增加任何检查开销，仅在需要时触发异常记录逻辑
2. **状态一致性**：确保[Meta](file:///e%3A/github/dbsyncer/dbsyncer-parser/src/main/java/org/dbsyncer/parser/model/Meta.java)对象中的异常状态与驱动实际状态保持一致
3. **错误恢复**：当用户手动启动驱动时，应清除之前的异常状态和异常信息

## 设计原则遵循

1. **奥卡姆剃刀原则**：最小化变更，复用现有全量和增量组件，仅增加协调逻辑，确保设计的简洁性和可维护性
2. **业务架构理解**：从业务架构角度关注核心业务实体及其领域属性的直接表达
3. **异常驱动模式**：强调零性能开销，在正常情况下不增加任何检查开销，仅在需要时触发异常记录逻辑
4. **简洁高效设计**：采用简洁高效的实现方式，反对冗余代码，倾向于简化逻辑

## 后续优化建议

1. 可以增加更详细的错误分类，如网络错误、数据库错误等，使用不同颜色标识
2. 提供错误详情查看功能，点击图标可查看详细错误日志
3. 增加错误统计功能，在驱动列表页面显示异常驱动数量