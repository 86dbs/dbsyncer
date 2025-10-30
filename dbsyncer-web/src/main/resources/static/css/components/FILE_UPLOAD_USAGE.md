# DBSyncer 文件上传组件使用文档

## 概述

这是一个现代化的文件上传组件，支持拖拽上传、多文件上传、实时进度显示、文件校验等功能。

## 特性

✅ **拖拽上传** - 支持将文件拖拽到上传区域  
✅ **点击选择** - 点击上传区域或按钮选择文件  
✅ **多文件上传** - 支持同时选择和上传多个文件  
✅ **进度显示** - 实时显示每个文件的上传进度  
✅ **文件校验** - 支持文件类型和大小限制  
✅ **错误处理** - 友好的错误提示和重试机制  
✅ **并发控制** - 限制同时上传的文件数量  
✅ **美观UI** - 现代化的渐变设计和流畅动画  

## 快速开始

### 1. HTML 结构

```html
<div id="myUploader" class="dbsyncer-upload-container">
    <div class="dbsyncer-upload-area" data-upload-area>
        <div class="dbsyncer-upload-icon">
            <i class="fa fa-cloud-upload"></i>
        </div>
        <div class="dbsyncer-upload-text">点击或拖拽文件到此处上传</div>
        <div class="dbsyncer-upload-hint">
            支持扩展名：.json, .xlsx, .csv<br>
            单次最多上传 10 个文件，每个文件不超过 50MB
        </div>
        <div class="dbsyncer-upload-button">选择文件</div>
        <input type="file" 
               class="dbsyncer-upload-input" 
               accept=".json,.xlsx,.csv"
               multiple>
    </div>
    <div class="dbsyncer-upload-list" data-upload-list></div>
</div>
```

### 2. JavaScript 初始化

```javascript
$(function() {
    if (window.DBSyncerTheme && DBSyncerTheme.initFileUpload) {
        var uploader = DBSyncerTheme.initFileUpload('#myUploader', {
            uploadUrl: '/api/upload',
            accept: ['.json', '.xlsx', '.csv'],
            maxFiles: 10,
            maxSize: 50 * 1024 * 1024, // 50MB
            autoUpload: true, // 选择后自动上传
            onSuccess: function(file, response) {
                console.log('上传成功：', file.name);
                DBSyncerTheme.notify({ 
                    message: '文件上传成功！', 
                    type: 'success' 
                });
            },
            onError: function(file, error) {
                console.error('上传失败：', file.name, error);
                DBSyncerTheme.notify({ 
                    message: '上传失败：' + error, 
                    type: 'danger' 
                });
            },
            onProgress: function(file, event) {
                console.log('上传进度：', file.name, file.progress + '%');
            }
        });
    }
});
```

## 配置选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `uploadUrl` | String | `'/upload'` | 上传接口地址 |
| `accept` | Array | `[]` | 允许的文件扩展名，如 `['.json', '.xlsx']` |
| `maxFiles` | Number | `10` | 最多可上传的文件数量 |
| `maxSize` | Number | `50*1024*1024` | 单个文件最大大小（字节） |
| `autoUpload` | Boolean | `true` | 是否选择后自动上传 |
| `onSuccess` | Function | - | 上传成功回调 `function(file, response)` |
| `onError` | Function | - | 上传失败回调 `function(file, error)` |
| `onProgress` | Function | - | 上传进度回调 `function(file, event)` |

## API 方法

初始化后返回的 uploader 实例提供以下方法：

```javascript
var uploader = DBSyncerTheme.initFileUpload('#myUploader', options);

// 获取所有文件列表
var files = uploader.getFiles();

// 清空所有文件
uploader.clearFiles();

// 手动上传所有待上传的文件（autoUpload 为 false 时使用）
uploader.uploadAll();
```

## 服务器端接口规范

### 请求格式

```
POST /api/upload
Content-Type: multipart/form-data

files: [File, File, ...]
```

### 响应格式

成功响应：
```json
{
  "success": true,
  "data": {
    "fileId": "123456",
    "fileName": "example.json",
    "fileUrl": "https://example.com/files/example.json"
  }
}
```

失败响应：
```json
{
  "success": false,
  "resultValue": "文件格式不正确"
}
```

## 使用示例

### 示例 1：图片上传

```javascript
DBSyncerTheme.initFileUpload('#imageUploader', {
    uploadUrl: '/api/upload/image',
    accept: ['.jpg', '.jpeg', '.png', '.gif'],
    maxFiles: 5,
    maxSize: 5 * 1024 * 1024, // 5MB
    onSuccess: function(file, response) {
        console.log('图片上传成功：', response.data.fileUrl);
        // 显示缩略图
        var img = '<img src="' + response.data.fileUrl + '" alt="' + file.name + '">';
        $('#preview').append(img);
    }
});
```

### 示例 2：JSON 配置文件上传

```javascript
DBSyncerTheme.initFileUpload('#configUploader', {
    uploadUrl: $basePath + '/config/upload',
    accept: ['.json'],
    maxFiles: 5,
    maxSize: 10 * 1024 * 1024, // 10MB
    onSuccess: function(file, response) {
        if (response.success) {
            DBSyncerTheme.notify({ message: '配置上传成功！', type: 'success' });
            setTimeout(function() {
                location.reload();
            }, 1000);
        }
    },
    onError: function(file, error) {
        DBSyncerTheme.notify({ message: error, type: 'danger' });
    }
});
```

### 示例 3：Excel 导入

```javascript
DBSyncerTheme.initFileUpload('#excelUploader', {
    uploadUrl: '/api/import/excel',
    accept: ['.xlsx', '.xls'],
    maxFiles: 1, // 只允许上传一个文件
    maxSize: 20 * 1024 * 1024, // 20MB
    onSuccess: function(file, response) {
        DBSyncerTheme.notify({ 
            message: '导入成功，共 ' + response.data.totalRows + ' 行数据', 
            type: 'success' 
        });
        // 刷新表格
        loadDataTable();
    }
});
```

### 示例 4：手动上传（不自动上传）

```html
<div id="manualUploader" class="dbsyncer-upload-container">
    <!-- ... 上传区域 ... -->
</div>
<button id="uploadBtn" class="btn btn-primary">开始上传</button>
```

```javascript
var uploader = DBSyncerTheme.initFileUpload('#manualUploader', {
    uploadUrl: '/api/upload',
    autoUpload: false, // 关闭自动上传
    onSuccess: function(file, response) {
        console.log('上传完成：', file.name);
    }
});

// 点击按钮手动上传
$('#uploadBtn').on('click', function() {
    uploader.uploadAll();
});
```

## 样式定制

可以通过 CSS 变量自定义组件样式：

```css
:root {
  --primary-color: #1890ff;
  --primary-hover: #40a9ff;
  --text-primary: #333;
  --text-secondary: #666;
  --text-tertiary: #999;
  --bg-primary: #fff;
  --border-primary: #d9d9d9;
}

/* 自定义上传区域样式 */
.dbsyncer-upload-area {
  border-color: your-color;
  background: your-gradient;
}

/* 自定义上传按钮 */
.dbsyncer-upload-button {
  background: your-color;
}
```

## 文件状态

组件会为每个文件设置不同的状态类：

- `.pending` - 待上传
- `.uploading` - 上传中
- `.success` - 上传成功
- `.error` - 上传失败

可以根据状态添加不同的样式：

```css
.dbsyncer-upload-item.uploading {
  /* 上传中的样式 */
}

.dbsyncer-upload-item.success {
  /* 成功的样式 */
}

.dbsyncer-upload-item.error {
  /* 失败的样式 */
}
```

## 常见问题

### Q: 如何限制只能上传一个文件？
A: 设置 `maxFiles: 1` 并移除 input 的 `multiple` 属性。

### Q: 如何获取上传后的文件 URL？
A: 在 `onSuccess` 回调中通过 `response.data` 获取服务器返回的数据。

### Q: 如何自定义上传请求头？
A: 目前不支持自定义请求头，如需此功能可扩展 `uploadFile` 函数。

### Q: 如何取消正在上传的文件？
A: 目前正在上传的文件无法删除，可以扩展功能保存 xhr 对象并调用 `xhr.abort()`。

### Q: 支持哪些浏览器？
A: 支持所有现代浏览器（Chrome, Firefox, Safari, Edge）和 IE11+。

## 注意事项

1. **服务器端接口**必须符合规范的响应格式
2. **文件大小限制**需同时在前端和后端设置
3. **CORS 问题**：如果上传到其他域，需要服务器配置 CORS
4. **并发上传**：默认最多同时上传 3 个文件，可修改 `uploadFile` 函数中的限制
5. **进度显示**：需要服务器支持进度事件（通常自动支持）

## 更新日志

### v1.0.0 (2025-10-30)
- ✨ 初始版本发布
- ✅ 支持拖拽上传
- ✅ 支持多文件上传
- ✅ 实时进度显示
- ✅ 文件校验
- ✅ 错误处理
- 
  🚀 下一步建议
  测试上传功能 - 确保服务器端接口返回正确的 JSON 格式
  应用到其他页面 - 如 plugin/plugin.html 等需要上传的页面
  扩展功能（可选）：
- ✅ 图片预览
- ✅ 大文件分片上传
- ✅ 断点续传
- ✅ 自定义请求头
## 相关链接

- [组件样式文件](./file-upload.css)
- [框架 JS 文件](../../js/framework.js)
- [使用示例](../../public/config/config.html)