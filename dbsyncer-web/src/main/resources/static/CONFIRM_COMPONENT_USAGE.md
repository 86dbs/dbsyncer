# Confirm 组件使用指南

## 📋 概述

`showConfirm` 是一个功能完整的确认对话框组件，具有以下特性：

- ✅ 支持多种图标类型（info、warning、error、success）
- ✅ 支持多种按钮样式（primary、success、warning、danger、info）
- ✅ 支持多种对话框大小（normal、large、max）
- ✅ 支持确认回调函数
- ✅ 点击非对话框区域自动关闭
- ✅ ESC 键关闭对话框
- ✅ 现代化设计，支持响应式

## 🚀 快速开始

### 基础用法

```javascript
showConfirm({
    title: '确认删除',
    message: '确定要删除这条记录吗？',
    icon: 'warning',
    onConfirm: function() {
        console.log('用户点击了确定');
        // 执行删除操作
    }
});
```

### 常见场景

#### 1. 普通确认（信息）
```javascript
showConfirm({
    title: '提示信息',
    message: '这是一条提示信息',
    icon: 'info',
    confirmText: '我知道了',
    onConfirm: function() {
        // 处理确认
    }
});
```

#### 2. 删除确认（危险操作）
```javascript
showConfirm({
    title: '确认删除',
    message: '删除后将无法恢复，请谨慎操作',
    icon: 'warning',
    confirmType: 'danger',
    confirmText: '确认删除',
    onConfirm: function() {
        // 执行删除
    }
});
```

#### 3. 成功确认
```javascript
showConfirm({
    title: '操作成功',
    message: '数据已保存',
    icon: 'success',
    confirmType: 'success',
    confirmText: '确定',
    onConfirm: function() {
        // 处理后续操作
    }
});
```

#### 4. 大对话框
```javascript
showConfirm({
    title: '用户协议',
    message: '请阅读以下条款',
    body: '<p>这里是详细的条款内容...</p>',
    size: 'large',
    icon: 'info',
    onConfirm: function() {
        // 用户同意
    }
});
```

#### 5. 最大对话框
```javascript
showConfirm({
    title: '详细信息',
    message: '请查看以下详细内容',
    body: '<div class="detailed-content">...</div>',
    size: 'max',
    onConfirm: function() {
        // 处理
    }
});
```

## 📖 完整参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `title` | String | 无 | **必需**，对话框标题 |
| `message` | String | '' | 对话框内容信息 |
| `body` | String | '' | 对话框主体HTML内容，可包含复杂HTML |
| `icon` | String | 'info' | 图标类型：`info`/`warning`/`error`/`success` |
| `confirmText` | String | '确定' | 确认按钮文本 |
| `cancelText` | String | '取消' | 取消按钮文本 |
| `confirmType` | String | 'primary' | 确认按钮样式：`primary`/`success`/`warning`/`danger`/`info` |
| `size` | String | 'normal' | 对话框大小：`normal`(400px)/`large`(500px)/`max`(600px) |
| `onConfirm` | Function | - | 确认按钮点击回调 |
| `onCancel` | Function | - | 取消按钮点击回调（可选） |

## 🎨 图标类型对照

| 类型 | 图标 | 常用场景 |
|------|------|--------|
| `info` | ℹ️ 信息 | 一般提示信息 |
| `warning` | ⚠️ 警告 | 警告、删除确认 |
| `error` | ❌ 错误 | 错误提示 |
| `success` | ✓ 成功 | 成功提示 |

## 🎨 按钮类型对照

| 类型 | 颜色 | 常用场景 |
|------|------|--------|
| `primary` | 蓝色 | 普通确认操作 |
| `success` | 绿色 | 成功/通过操作 |
| `warning` | 橙色 | 警告/注意操作 |
| `danger` | 红色 | 危险/删除操作 |
| `info` | 蓝色 | 信息提示 |

## 📏 对话框大小

### normal (常规)
```javascript
showConfirm({
    title: '标题',
    size: 'normal'  // 宽度: 400px
});
```

### large (较大)
```javascript
showConfirm({
    title: '标题',
    size: 'large'   // 宽度: 500px
});
```

### max (最大)
```javascript
showConfirm({
    title: '标题',
    size: 'max'     // 宽度: 600px
});
```

## 🔄 用户交互

### 关闭对话框的方式

1. **点击确认按钮** → 执行 `onConfirm` 回调
2. **点击取消按钮** → 执行 `onCancel` 回调（如果有）
3. **点击非对话框区域** → 执行 `onCancel` 回调（如果有）
4. **按 ESC 键** → 执行 `onCancel` 回调（如果有）

## 💡 实际应用示例

### 示例1：删除用户
```javascript
showConfirm({
    title: '确认删除',
    message: '确定要删除用户 "张三" 吗？',
    icon: 'warning',
    confirmType: 'danger',
    confirmText: '确认删除',
    cancelText: '取消',
    onConfirm: function() {
        // 调用删除接口
        doPoster('/user/delete', { userId: 123 }, function(data) {
            if (data.success) {
                bootGrowl('删除成功', 'success');
                // 刷新列表
            }
        });
    }
});
```

### 示例2：确认提交表单
```javascript
document.getElementById('submitBtn').addEventListener('click', function() {
    showConfirm({
        title: '提交确认',
        message: '确定要提交这个表单吗？',
        icon: 'info',
        confirmText: '确认提交',
        onConfirm: function() {
            // 提交表单
            const formData = document.getElementById('myForm').serializeJson();
            doPoster('/save', formData, function(data) {
                if (data.success) {
                    bootGrowl('保存成功', 'success');
                }
            });
        }
    });
});
```

### 示例3：显示详细信息
```javascript
showConfirm({
    title: '用户详情',
    message: '请查看以下用户信息',
    body: `
        <div style="padding: 10px;">
            <p><strong>姓名:</strong> 张三</p>
            <p><strong>邮箱:</strong> zhangsan@example.com</p>
            <p><strong>部门:</strong> 技术部</p>
            <p><strong>状态:</strong> <span style="color: green;">活跃</span></p>
        </div>
    `,
    size: 'normal',
    icon: 'info',
    confirmText: '关闭',
    onConfirm: function() {
        console.log('用户已关闭详情窗口');
    }
});
```

### 示例4：使用 onCancel 回调
```javascript
showConfirm({
    title: '保存更改',
    message: '您有未保存的更改，是否保存？',
    icon: 'warning',
    confirmText: '保存',
    cancelText: '不保存',
    onConfirm: function() {
        // 保存更改
        console.log('保存更改');
    },
    onCancel: function() {
        // 放弃更改
        console.log('放弃更改');
    }
});
```

## 🎯 最佳实践

1. **明确的标题** - 使用清晰的标题说明操作目的
2. **适当的图标** - 根据操作类型选择合适的图标
3. **合适的按钮样式** - 危险操作使用 `danger` 类型
4. **简洁的文本** - 保持信息简洁明了
5. **必要时添加 body** - 对于复杂信息使用 `body` 字段

## 📝 注意事项

- `title` 参数是必需的
- 图标和按钮文本会自动转义，防止 XSS 攻击
- `body` 参数中的 HTML 不会被转义，请确保内容安全
- 对话框会自动清理事件监听器，不会造成内存泄漏
- 在移动设备上会自动调整对话框大小

## 🐛 故障排除

### 问题：对话框没有显示
**解决方案：** 确保已导入 confirm.css 和 framework.js

### 问题：回调函数没有执行
**解决方案：** 确保 `onConfirm` 是一个有效的函数

### 问题：样式不正确
**解决方案：** 清空浏览器缓存，重新加载页面

## 📞 支持

如有问题或建议，请联系开发团队。

