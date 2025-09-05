# Mapping扩展参数下拉选择功能实现方案

## 1. 现状分析

### 1.1 当前实现
- **前端**: `editCommon.html` 中参数键使用 `<input type="text">` 直接编辑
- **后端**: `AbstractConfigModel.params` 为 `Map<String, String>` 结构
- **数据流**: 前端JSON -> `MappingChecker.modifyConfigModel()` -> `AbstractConfigModel.setParams()`

### 1.2 存在问题
1. 参数键容易拼写错误
2. 无法统一管理可用参数
3. 缺乏参数说明和验证
4. 用户不清楚可以配置哪些参数

## 2. 改进方案

### 2.1 总体架构
```
前端下拉选择 -> 参数枚举定义 -> 后端API支持 -> 数据验证
```

### 2.2 后端改动

#### 2.2.1 创建参数枚举类
**位置**: `dbsyncer-sdk/src/main/java/org/dbsyncer/sdk/enums/ParamKeyEnum.java`

```java
public enum ParamKeyEnum {
    // Kafka 目标连接器
    TOPIC("topic", "消息主题", "String", "设置Kafka消息主题名称", false),

    // 所有目标连接器
    TOPIC("table.missCreate", "缺失自动创建", "bool", "目标数据表如果缺失则自动创建", false),
    
    private final String key;
    private final String name;
    private final String type;
    private final String description;
    private final boolean required;
    
    // 构造函数、getter方法...
}
```

#### 2.2.2 创建参数配置服务
**位置**: `dbsyncer-biz/src/main/java/org/dbsyncer/biz/ParamConfigService.java`

```java
@Service
public class ParamConfigService {
    
    /**
     * 获取所有可用参数配置
     */
    public List<ParamConfigVo> getAllParamConfigs() {
        return Arrays.stream(ParamKeyEnum.values())
                .map(this::convertToVo)
                .collect(Collectors.toList());
    }
    
    /**
     * 根据连接器类型获取相关参数
     */
    public List<ParamConfigVo> getParamsByConnectorType(String connectorType) {
        // 根据连接器类型过滤相关参数
    }
    
    /**
     * 验证参数键值对
     */
    public void validateParams(Map<String, String> params) {
        // 验证参数合法性
    }
}
```

#### 2.2.3 创建Web API
**位置**: `dbsyncer-web/src/main/java/org/dbsyncer/web/controller/config/ParamController.java`

```java
@Controller
@RequestMapping("/param")
public class ParamController {
    
    @Resource
    private ParamConfigService paramConfigService;
    
    @GetMapping("/getParamOptions.json")
    @ResponseBody
    public RestResult getParamOptions(@RequestParam(required = false) String connectorType) {
        List<ParamConfigVo> paramConfigs = StringUtil.isNotBlank(connectorType) 
            ? paramConfigService.getParamsByConnectorType(connectorType)
            : paramConfigService.getAllParamConfigs();
        return RestResult.restSuccess(paramConfigs);
    }
}
```

### 2.3 前端改动

#### 2.3.1 修改HTML模板
**文件**: `editCommon.html`

将参数键输入框改为下拉选择：
```html
<td>
    <select class="form-control common-param-key selectpicker" 
            data-live-search="true" 
            data-size="10"
            th:attr="data-selected=${entry.key}">
        <option value="">请选择参数键</option>
        <!-- 动态加载选项 -->
    </select>
</td>
```

#### 2.3.2 增强JavaScript功能
1. **加载参数选项**: 页面初始化时调用API获取参数列表
2. **动态筛选**: 根据连接器类型筛选显示相关参数
3. **参数说明**: 鼠标悬停显示参数描述
4. **重复检测**: 防止添加重复参数键

```javascript
// 加载参数选项
function loadParamOptions() {
    $.get('/param/getParamOptions.json', function(result) {
        if (result.flag) {
            buildParamKeyOptions(result.data);
        }
    });
}

// 构建参数键选项
function buildParamKeyOptions(paramConfigs) {
    var optionsHtml = '<option value="">请选择参数键</option>';
    paramConfigs.forEach(function(param) {
        optionsHtml += `<option value="${param.key}" 
                               title="${param.description}" 
                               data-type="${param.type}"
                               data-required="${param.required}">${param.name}</option>`;
    });
    
    $('.common-param-key').html(optionsHtml).selectpicker('refresh');
}
```

### 2.4 数据模型定义

#### 2.4.1 VO类
**位置**: `dbsyncer-biz/src/main/java/org/dbsyncer/biz/vo/ParamConfigVo.java`

```java
public class ParamConfigVo {
    private String key;        // 参数键
    private String name;       // 参数名称
    private String type;       // 参数类型
    private String description; // 参数描述
    private boolean required;   // 是否必填
    private List<String> options; // 可选值列表(下拉值)
    
    // getter/setter...
}
```

## 3. 实施步骤

### 阶段一：后端基础设施 (1-2天)
1. ✅ 创建 `ParamKeyEnum` 枚举类
2. ✅ 创建 `ParamConfigVo` 数据模型
3. ✅ 实现 `ParamConfigService` 服务类
4. ✅ 创建 `ParamController` Web接口
5. ✅ 添加参数验证逻辑

### 阶段二：前端界面改造 (1天)
1. ✅ 修改 `editCommon.html` 模板
2. ✅ 更新JavaScript逻辑
3. ✅ 增加参数描述展示
4. ✅ 实现重复参数检测

### 阶段三：集成测试 (0.5天)
1. ✅ 功能测试
2. ✅ 兼容性测试
3. ✅ 界面优化

## 4. 技术细节

### 4.1 参数分类策略
- **通用参数**: 适用于所有连接器类型
- **特定参数**: 仅适用于特定连接器类型
- **可选参数**: 根据业务需求添加

### 4.2 扩展性考虑
- 支持动态添加新参数类型
- 支持参数依赖关系配置
- 支持参数值校验规则

### 4.3 兼容性保证
- 保持现有API接口不变
- 支持手动输入模式(兼容旧版本)
- 渐进式升级策略

## 5. 预期效果

### 5.1 用户体验提升
- ✅ 减少参数配置错误
- ✅ 提供参数使用指导
- ✅ 简化配置流程

### 5.2 系统稳定性
- ✅ 统一参数管理
- ✅ 增强数据验证
- ✅ 减少配置错误

### 5.3 维护便利性
- ✅ 集中参数定义
- ✅ 便于功能扩展
- ✅ 降低维护成本

## 6. 风险评估

### 6.1 技术风险
- **风险**: 参数枚举维护成本
- **缓解**: 设计良好的扩展机制

### 6.2 兼容性风险  
- **风险**: 现有配置数据兼容
- **缓解**: 保持现有数据结构不变

### 6.3 性能风险
- **风险**: 参数加载性能影响
- **缓解**: 实现缓存机制

## 7. 后续优化方向

1. **参数模板**: 为常用场景提供参数模板
2. **参数依赖**: 支持参数间依赖关系
3. **动态验证**: 实时参数值校验
4. **参数导入**: 支持从文件导入参数配置
5. **参数历史**: 记录参数修改历史