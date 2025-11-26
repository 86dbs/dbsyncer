# Framework.js 组件拆分总结

## 拆分完成时间
2024年

## 拆分清单

### 已拆分的组件

1. **confirm.js** - 确认对话框组件
   - 函数：`showConfirm(options)`
   - 位置：`/js/components/confirm.js`
   - 依赖：无

2. **db-select.js** - jQuery 下拉选择组件
   - 插件：`$.fn.dbSelect(options)`
   - 位置：`/js/components/db-select.js`
   - 依赖：jQuery
   - 全局变量：`window.dbSelectCurrentOpenInstance`

3. **pagination-manager.js** - 通用分页管理器
   - 构造函数：`PaginationManager(options)`
   - 位置：`/js/components/pagination-manager.js`
   - 依赖：`doPoster`, `bootGrowl`, jQuery

4. **multi-select.js** - 多选下拉框组件
   - 函数：`initMultiSelect(wrapperId, options)`
   - 函数：`autoMatchSimilarItems(sourceWrapperId, targetWrapperId)`
   - 位置：`/js/components/multi-select.js`
   - 依赖：jQuery

5. **qrcode-popover.js** - 二维码悬浮提示组件
   - 函数：`initQRCodePopover(options)`
   - 位置：`/js/components/qrcode-popover.js`
   - 依赖：QRCode 库

6. **notify.js** - 通知/Toast 组件
   - 函数：`notify(message, type, options)`
   - 函数：`removeToast(toast)`
   - 函数：`bootGrowl(message, type, duration)`
   - 函数：`ensureToastContainer()`
   - 位置：`/js/components/notify.js`
   - 依赖：无

7. **tags-input.js** - 标签输入框组件
   - 函数：`initMultipleInputTags()`
   - 位置：`/js/components/tags-input.js`
   - 依赖：无

8. **file-upload.js** - 文件上传组件
   - 函数：`initFileUpload(selector, options)`
   - 位置：`/js/components/file-upload.js`
   - 依赖：`notify` 函数

## 引入顺序

在 HTML 中按以下顺序引入组件文件：

```html
<!-- 1. 通知组件（其他组件可能依赖） -->
<script th:src="@{/js/components/notify.js}"></script>

<!-- 2. 确认对话框 -->
<script th:src="@{/js/components/confirm.js}"></script>

<!-- 3. 分页管理器 -->
<script th:src="@{/js/components/pagination-manager.js}"></script>

<!-- 4. 下拉选择组件 -->
<script th:src="@{/js/components/db-select.js}"></script>

<!-- 5. 多选下拉框 -->
<script th:src="@{/js/components/multi-select.js}"></script>

<!-- 6. 二维码悬浮提示 -->
<script th:src="@{/js/components/qrcode-popover.js}"></script>

<!-- 7. 标签输入框 -->
<script th:src="@{/js/components/tags-input.js}"></script>

<!-- 8. 文件上传组件 -->
<script th:src="@{/js/components/file-upload.js}"></script>

<!-- 9. 核心框架（最后引入） -->
<script th:src="@{/js/framework.js}"></script>
```

## Framework.js 保留内容

以下内容仍保留在 `framework.js` 中：

- 初始化代码（`$basePath`, `$mainContent` 等）
- 工具函数（`showLoading`, `hideLoading`, `showEmpty`, `escapeHtml`, `formatDate`, `isBlank`, `splitStrByDelimiter`）
- 水印功能（`watermark()`）
- 表单扩展方法（`$.fn.serializeJson`, `$.fn.formValidate`）
- 全局页面加载（`doLoader`）
- Ajax 请求（`doRequest`, `doErrorResponse`, `doPoster`, `doGetter`）
- 搜索初始化（`initSearch`）
- 表单验证（`validateForm`）
- 注销功能（`logout`）
- 用户信息刷新（`refreshLoginUser`, `refreshLicense`）
- 下拉菜单处理
- DBSyncerTheme 全局对象导出

## 注意事项

1. **依赖关系**：
   - `pagination-manager.js` 依赖 `doPoster` 和 `bootGrowl`
   - `file-upload.js` 依赖 `notify`
   - `db-select.js` 依赖 jQuery

2. **全局函数**：
   - 所有组件函数都导出到 `window` 对象
   - `framework.js` 中的函数会检查组件是否已加载，如果未加载会显示警告

3. **向后兼容**：
   - `framework.js` 中保留了函数占位符，确保向后兼容
   - 如果组件文件未引入，会显示警告信息

4. **更新 HTML 模板**：
   - 需要在所有使用这些组件的 HTML 页面中添加组件文件的引用
   - 建议在公共模板（如 `index.html`）中统一引入

## 文件结构

```
dbsyncer-web/src/main/resources/static/js/
├── components/
│   ├── confirm.js
│   ├── db-select.js
│   ├── pagination-manager.js
│   ├── multi-select.js
│   ├── qrcode-popover.js
│   ├── notify.js
│   ├── tags-input.js
│   └── file-upload.js
└── framework.js
```

