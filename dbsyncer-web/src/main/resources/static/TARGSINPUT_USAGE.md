# 标签输入框（Tagsinput）组件使用说明

## 功能概述

`initMultipleInputTags` 是一个轻量级的标签输入框组件，将普通的 `<input>` 元素转换为支持多个值的标签输入框。每个标签都有精美的渐变背景色和字体颜色，用户可以方便地添加和删除标签，提交表单时自动用逗号拼接所有标签值。

## 特性

✅ **主要功能**：
- 🏷️ 将普通输入框转换为标签输入框
- ➕ 支持添加多个标签
- ❌ 支持删除标签
- 🎨 渐变背景色和白色字体
- ⌨️ 键盘快捷操作（Enter、逗号、退格键）
- 📤 表单提交时自动用逗号拼接
- 🔄 支持初始值（从 `value` 或 `th:value` 读取）
- 🚫 自动去重（不允许添加重复的标签）
- 💫 悬停动画效果

## 基本用法

### 1. HTML 结构

在输入框上添加 `data-role="tagsinput"` 属性：

```html
<!-- 基本用法 -->
<input type="text" 
       name="email" 
       class="form-control" 
       data-role="tagsinput" 
       placeholder="请输入邮箱地址"/>

<!-- 带初始值（服务端渲染）-->
<input type="text" 
       name="email" 
       class="form-control" 
       data-role="tagsinput" 
       th:value="${userInfo?.email}" 
       placeholder="请输入邮箱地址"/>

<!-- 带初始值（静态 HTML）-->
<input type="text" 
       name="tags" 
       class="form-control" 
       data-role="tagsinput" 
       value="标签1,标签2,标签3" 
       placeholder="请输入标签"/>
```

### 2. JavaScript 初始化

在页面的 JavaScript 中调用初始化函数：

```javascript
$(function () {
    // 方式 1: 直接调用全局函数
    initMultipleInputTags();
    
    // 方式 2: 通过 DBSyncerTheme 命名空间调用
    if (window.DBSyncerTheme && DBSyncerTheme.initMultipleInputTags) {
        DBSyncerTheme.initMultipleInputTags();
    }
});
```

### 3. 表单提交

标签输入框的值会自动更新到原始的隐藏输入框中，以逗号分隔：

```javascript
// 假设用户添加了 3 个邮箱标签：
// user1@example.com
// user2@example.com
// user3@example.com

// 表单提交时，输入框的值为：
// "user1@example.com,user2@example.com,user3@example.com"

$("#myForm").on('submit', function(e) {
    e.preventDefault();
    const data = $(this).serializeJson();
    console.log(data.email); // "user1@example.com,user2@example.com,user3@example.com"
});
```

## 使用示例

### 示例 1: 邮箱输入（许可证页面）

```html
<div class="form-item">
    <label class="form-label">邮箱
        <i class="fa fa-question-circle text-tertiary" title="支持多个邮箱"></i>
    </label>
    <div class="form-control-area">
        <input class="form-control" 
               type="text" 
               name="email" 
               data-role="tagsinput" 
               th:value="${userInfo?.email}" 
               placeholder="请输入邮箱地址"/>
        <div class="form-help">支持多个邮箱，按 Enter 键或逗号添加，提交时自动用逗号拼接</div>
    </div>
</div>

<script>
$(function() {
    initMultipleInputTags();
});
</script>
```

### 示例 2: 用户管理（添加用户）

```html
<div class="form-item">
    <label class="form-label">邮箱</label>
    <div class="form-control-area">
        <input type="text" 
               class="form-control" 
               name="email" 
               data-role="tagsinput" 
               placeholder="请输入邮箱地址"/>
        <div class="form-help">支持多个邮箱，按 Enter 键添加</div>
    </div>
</div>

<script>
$(function() {
    initMultipleInputTags();
});
</script>
```

### 示例 3: 标签管理

```html
<div class="form-group">
    <label>文章标签</label>
    <input type="text" 
           class="form-control" 
           name="tags" 
           data-role="tagsinput" 
           value="技术,编程,数据库"
           placeholder="请输入标签"/>
    <small class="text-muted">按 Enter 键或输入逗号添加标签</small>
</div>

<script>
$(function() {
    initMultipleInputTags();
});
</script>
```

### 示例 4: 数据库主键配置（Mapping 页面）

```html
<input id="sourceTablePK" 
       class="form-control" 
       type="text" 
       data-role="tagsinput"
       placeholder="请输入主键字段"/>

<script>
$(function() {
    initMultipleInputTags();
});
</script>
```

## 键盘操作

| 按键 | 功能 |
|------|------|
| **Enter** | 添加当前输入框中的内容为标签 |
| **,**（逗号）| 添加当前输入框中的内容为标签 |
| **Backspace** | 输入框为空时，删除最后一个标签 |
| **Tab** | 正常的表单导航 |

## 鼠标操作

| 操作 | 功能 |
|------|------|
| **点击容器** | 聚焦到输入框 |
| **点击标签的 ×** | 删除该标签 |
| **悬停在标签上** | 标签上浮并显示阴影 |

## 样式定制

### 基本样式变量

组件的样式在 `dbsyncer-theme.css` 中定义，可以通过覆盖 CSS 变量来自定义样式：

```css
/* 修改标签背景渐变 */
.dbsyncer-tag {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

/* 修改标签悬停效果 */
.dbsyncer-tag:hover {
  background: linear-gradient(135deg, #5568d3 0%, #6a3f8e 100%);
  transform: translateY(-1px);
  box-shadow: 0 2px 6px rgba(102, 126, 234, 0.3);
}
```

### 使用预定义的颜色变体

组件提供了 5 种预定义的颜色变体：

```css
/* 主色调（蓝色） */
.dbsyncer-tag.tag-primary {
  background: linear-gradient(135deg, #1890ff 0%, #096dd9 100%);
}

/* 成功（绿色） */
.dbsyncer-tag.tag-success {
  background: linear-gradient(135deg, #52c41a 0%, #389e0d 100%);
}

/* 警告（黄色） */
.dbsyncer-tag.tag-warning {
  background: linear-gradient(135deg, #faad14 0%, #d48806 100%);
}

/* 危险（红色） */
.dbsyncer-tag.tag-danger {
  background: linear-gradient(135deg, #ff4d4f 0%, #cf1322 100%);
}

/* 信息（青色） */
.dbsyncer-tag.tag-info {
  background: linear-gradient(135deg, #13c2c2 0%, #08979c 100%);
}
```

要使用这些变体，可以在 JavaScript 中动态添加 class：

```javascript
// 修改 addTag 函数，添加颜色 class
function addTag(value, colorClass) {
    // ...
    tagElement.className = 'dbsyncer-tag ' + (colorClass || '');
    // ...
}
```

### 自定义样式示例

```css
/* 圆形标签 */
.dbsyncer-tag {
  border-radius: 20px;
}

/* 更大的标签 */
.dbsyncer-tag {
  padding: 6px 12px;
  font-size: 14px;
}

/* 纯色背景（不使用渐变） */
.dbsyncer-tag {
  background: #1890ff;
}

/* 修改容器边框颜色 */
.dbsyncer-tagsinput {
  border-color: #d9d9d9;
}

.dbsyncer-tagsinput:focus-within {
  border-color: #40a9ff;
}
```

## API 参考

### initMultipleInputTags()

初始化所有带有 `data-role="tagsinput"` 属性的输入框。

**语法：**
```javascript
initMultipleInputTags();
```

**参数：**
- 无

**返回值：**
- 无

**用法：**
```javascript
// 页面加载完成后初始化
$(function() {
    initMultipleInputTags();
});

// 动态加载内容后重新初始化
doLoader("/some-page", function() {
    initMultipleInputTags();
});
```

## 数据处理

### 读取标签值

```javascript
// 获取输入框的值（逗号分隔的字符串）
const tagsString = $('input[name="tags"]').val();
console.log(tagsString); // "标签1,标签2,标签3"

// 转换为数组
const tagsArray = tagsString.split(',').map(tag => tag.trim());
console.log(tagsArray); // ["标签1", "标签2", "标签3"]
```

### 设置标签值

```javascript
// 方式 1: 直接设置 value（在初始化前）
$('input[name="tags"]').val('标签1,标签2,标签3');
initMultipleInputTags();

// 方式 2: 服务端设置
<input data-role="tagsinput" th:value="${tags}" />
```

### 表单序列化

```javascript
// 使用 serializeJson（项目自定义方法）
const data = $('#myForm').serializeJson();
console.log(data.tags); // "标签1,标签2,标签3"

// 使用原生 serialize
const formData = $('#myForm').serialize();
console.log(formData); // "tags=标签1,标签2,标签3&..."

// 使用 FormData API
const formData = new FormData($('#myForm')[0]);
console.log(formData.get('tags')); // "标签1,标签2,标签3"
```

## 技术细节

### 工作原理

1. **查找元素**：
    - 查找所有带有 `data-role="tagsinput"` 的 `<input>` 元素

2. **创建结构**：
    - 创建标签容器（`.dbsyncer-tagsinput`）
    - 创建标签列表（`.dbsyncer-tagsinput-tags`）
    - 创建新标签输入框（`.dbsyncer-tagsinput-input`）
    - 隐藏原始输入框

3. **初始化值**：
    - 读取原始输入框的 `value` 或 `value` 属性
    - 按逗号分隔，去除空格
    - 为每个值创建标签

4. **事件绑定**：
    - 监听新标签输入框的键盘事件（Enter、逗号、退格键）
    - 监听失焦事件（添加未完成的标签）
    - 监听容器点击事件（聚焦到输入框）
    - 监听标签删除按钮点击事件

5. **数据同步**：
    - 每次添加或删除标签时，更新原始输入框的值
    - 用逗号拼接所有标签值

### HTML 转义

组件会自动对标签文本进行 HTML 转义，防止 XSS 攻击：

```javascript
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
```

### 去重逻辑

组件会检查标签是否已存在，防止添加重复的标签：

```javascript
if (tags.indexOf(value) !== -1) {
    return; // 标签已存在，不添加
}
```

## 浏览器兼容性

| 浏览器 | 版本 | 支持情况 |
|--------|------|----------|
| Chrome | 60+ | ✅ 完全支持 |
| Firefox | 55+ | ✅ 完全支持 |
| Safari | 11+ | ✅ 完全支持 |
| Edge | 79+ | ✅ 完全支持 |
| IE | 11 | ⚠️ 需要 polyfill（`forEach`、`classList` 等） |

## 常见问题

### Q1: 标签没有显示？

**A**: 检查以下几点：
1. 确保已调用 `initMultipleInputTags()`
2. 确保输入框有 `data-role="tagsinput"` 属性
3. 检查浏览器控制台是否有错误
4. 确保 CSS 文件已正确引入

### Q2: 如何修改标签颜色？

**A**: 覆盖 `.dbsyncer-tag` 的 CSS：
```css
.dbsyncer-tag {
  background: linear-gradient(135deg, #your-color-1 0%, #your-color-2 100%);
}
```

### Q3: 提交表单时值为空？

**A**: 确保：
1. 原始输入框有 `name` 属性
2. 标签已成功添加（检查原始输入框的 `value`）
3. 表单序列化方法正确

### Q4: 动态加载的输入框无法初始化？

**A**: 在内容加载完成后重新调用 `initMultipleInputTags()`：
```javascript
doLoader("/some-page", function() {
    initMultipleInputTags();
});
```

### Q5: 如何获取所有标签的数组？

**A**:
```javascript
const tagsString = $('input[name="tags"]').val();
const tagsArray = tagsString ? tagsString.split(',').map(tag => tag.trim()) : [];
```

### Q6: 如何限制标签数量？

**A**: 修改 `addTag` 函数，添加数量检查：
```javascript
function addTag(value) {
    if (tags.length >= 5) {
        alert('最多只能添加 5 个标签');
        return;
    }
    // ...原有逻辑
}
```

### Q7: 如何验证标签格式（如邮箱）？

**A**: 在 `addTag` 函数中添加验证：
```javascript
function addTag(value) {
    value = value.trim();
    if (!value) return;
    
    // 邮箱格式验证
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(value)) {
        alert('请输入有效的邮箱地址');
        return;
    }
    
    // ...原有逻辑
}
```

## 进阶用法

### 动态设置标签值

```javascript
// 清空所有标签并设置新值
function setTags(inputName, tagsArray) {
    const $input = $('input[name="' + inputName + '"]');
    $input.val(tagsArray.join(','));
    
    // 重新初始化
    const container = $input.prev('.dbsyncer-tagsinput');
    if (container.length) {
        container.remove();
    }
    $input.show().removeClass('tagsinput-initialized');
    initMultipleInputTags();
}

// 使用
setTags('email', ['user1@example.com', 'user2@example.com']);
```

### 监听标签变化

由于组件会更新原始输入框的值，可以监听 `change` 事件：

```javascript
$('input[name="tags"]').on('change', function() {
    const tags = $(this).val();
    console.log('标签已更改:', tags);
});
```

### 编程方式添加标签

```javascript
// 获取标签容器
const $input = $('input[name="tags"]');
const container = $input.prev('.dbsyncer-tagsinput');

// 模拟添加标签（需要修改组件以暴露 addTag 方法）
// 或者直接修改 value 并重新初始化
const currentTags = $input.val().split(',').filter(t => t);
currentTags.push('新标签');
$input.val(currentTags.join(','));
```

## 最佳实践

### 1. 页面加载时初始化

```javascript
$(function() {
    initMultipleInputTags();
});
```

### 2. 动态内容加载后重新初始化

```javascript
function loadContent(url) {
    doLoader(url, function() {
        // 重新初始化所有组件
        initMultipleInputTags();
        enhanceSelects();
    });
}
```

### 3. 表单验证

```javascript
$('#myForm').on('submit', function(e) {
    e.preventDefault();
    
    const tags = $('input[name="tags"]').val();
    if (!tags || tags.split(',').length === 0) {
        alert('请至少添加一个标签');
        return;
    }
    
    // 提交表单
    const data = $(this).serializeJson();
    submitForm(data);
});
```

### 4. 用户友好的提示

```html
<div class="form-help">
    <i class="fa fa-info-circle"></i> 
    支持多个邮箱，按 Enter 键或逗号添加，提交时自动用逗号拼接
</div>
```

## 已知限制

1. **不支持拖拽排序**：标签的顺序是添加顺序，不支持拖拽调整
2. **不支持分组**：所有标签在同一个容器中
3. **不支持自动完成**：没有下拉建议功能
4. **不支持标签编辑**：只能删除后重新添加

## 未来计划

- [ ] 添加拖拽排序功能
- [ ] 添加自动完成（下拉建议）
- [ ] 添加标签编辑功能
- [ ] 添加标签分组功能
- [ ] 添加最大标签数量限制配置
- [ ] 添加自定义验证规则配置
- [ ] 添加更多预定义颜色主题

## 更新日志

### v1.0.0 (2025-10-30)
- ✅ 初始版本
- ✅ 支持添加/删除标签
- ✅ 支持键盘快捷操作
- ✅ 支持初始值
- ✅ 渐变背景色
- ✅ 自动去重
- ✅ 表单提交时逗号拼接
- ✅ HTML 转义防 XSS

## 许可证

MIT License

## 作者

DBSyncer Team

## 相关文档

- [Forms 组件文档](./components/forms.css)
- [Framework.js API](./framework.js)
- [DBSyncer Theme](../css/dbsyncer-theme.css)

