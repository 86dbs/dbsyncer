# 二维码悬浮提示组件使用说明

## 功能概述

`initQRCodePopover` 是一个轻量级的二维码悬浮提示组件，当用户鼠标悬停在指定元素上时，会在其附近显示一个二维码弹窗。

## 依赖

- **QRCode.js**: `/plugins/js/qrcodejs/qrcode.min.js`
- **jQuery**: 可选，用于事件绑定
- **framework.js**: 主框架文件

## 基本用法

### 1. HTML 结构

在页面中添加需要触发二维码显示的元素，并添加统一的 class（如 `.qrcode-trigger`）：

```html
<a href="https://work.weixin.qq.com/u/vc7f073c9f993bc776" 
   class="btn btn-primary qrcode-trigger"
   target="_blank">
    <i class="fa fa-wechat"></i> 联系客服
</a>

<a href="https://work.weixin.qq.com/u/vc7f073c9f993bc776" 
   class="text-success qrcode-trigger">
    <i class="fa fa-wechat"></i> 星河同步官方
</a>
```

### 2. 引入依赖

确保在页面中引入必要的库：

```html
<!-- QRCode 库 -->
<script th:src="@{/plugins/js/qrcodejs/qrcode.min.js}"></script>

<!-- 框架 JS -->
<script th:src="@{/js/framework.js}"></script>

<!-- 页面 JS -->
<script th:src="@{/js/your-page.js}"></script>
```

### 3. JavaScript 初始化

在页面的 JavaScript 文件中调用初始化函数：

```javascript
$(function () {
    // 初始化二维码悬浮提示
    if (window.DBSyncerTheme && DBSyncerTheme.initQRCodePopover) {
        DBSyncerTheme.initQRCodePopover({
            url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776?v=4.1.20.26620',
            selector: '.qrcode-trigger',
            size: 150,
            position: 'bottom'
        });
    }
});
```

## 配置选项

### options (Object)

| 参数 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| `url` | `string` | `''` | ✅ | 二维码链接地址 |
| `selector` | `string` | `''` | ✅ | 目标元素选择器（支持多个元素） |
| `size` | `number` | `150` | ❌ | 二维码大小（像素） |
| `position` | `string` | `'bottom'` | ❌ | 弹窗位置：`bottom`/`top`/`left`/`right` |

## 示例

### 示例 1: 基本用法

```javascript
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.wechat-contact',
    size: 150,
    position: 'bottom'
});
```

### 示例 2: 多个元素

```javascript
// 多个元素使用同一个二维码
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.contact-btn, .contact-link, .contact-icon',
    size: 180,
    position: 'bottom'
});
```

### 示例 3: 不同位置

```javascript
// 顶部显示
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.bottom-contact',
    position: 'top'
});

// 左侧显示
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.right-contact',
    position: 'left'
});
```

### 示例 4: 自定义二维码大小

```javascript
// 大尺寸二维码
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.large-qrcode',
    size: 200,
    position: 'bottom'
});

// 小尺寸二维码
DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.small-qrcode',
    size: 120,
    position: 'bottom'
});
```

## 样式定制

组件的样式在 `dbsyncer-theme.css` 中定义，可以通过覆盖以下 CSS 类来自定义样式：

### 主要 CSS 类

```css
/* 弹窗容器 */
.dbsyncer-qrcode-popover {
  /* 可自定义 z-index, opacity, transform 等 */
}

/* 弹窗内容 */
.dbsyncer-qrcode-popover-inner {
  /* 可自定义 background, border-radius, box-shadow, padding 等 */
}

/* 二维码图片 */
.dbsyncer-qrcode-popover-inner img {
  /* 可自定义 width, height, border-radius 等 */
}

/* 提示文字 */
.dbsyncer-qrcode-text {
  /* 可自定义 font-size, color, font-weight 等 */
}

/* 箭头 */
.dbsyncer-qrcode-arrow {
  /* 可自定义箭头样式 */
}
```

### 自定义示例

```css
/* 修改弹窗背景色 */
.dbsyncer-qrcode-popover-inner {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

/* 修改提示文字颜色 */
.dbsyncer-qrcode-text {
  color: #ffffff;
}

/* 修改二维码圆角 */
.dbsyncer-qrcode-popover-inner img {
  border-radius: 8px;
}
```

## 特性

### ✅ 已实现功能

- ✅ 鼠标悬停显示二维码
- ✅ 支持多个元素共用同一个二维码
- ✅ 支持 4 个方向显示（上/下/左/右）
- ✅ 平滑的淡入淡出动画
- ✅ 自动计算位置，居中显示
- ✅ 悬停在弹窗上不会关闭
- ✅ 延迟显示/隐藏，避免误触
- ✅ 响应式设计，自适应页面滚动
- ✅ 高 z-index，确保在最上层显示

### ⏱️ 交互时机

- **延迟显示**: 鼠标悬停 **300ms** 后显示二维码
- **延迟隐藏**: 鼠标离开 **200ms** 后隐藏二维码
- **二维码生成**: 组件初始化后 **500ms** 生成二维码

## 技术细节

### 工作原理

1. **生成二维码**:
   - 在隐藏容器中使用 QRCode.js 生成二维码图片
   - 提取二维码图片的 Data URL

2. **创建弹窗**:
   - 为每个匹配的元素创建独立的弹窗 DOM
   - 弹窗初始状态为隐藏（opacity: 0, visibility: hidden）

3. **事件绑定**:
   - 监听目标元素的 `mouseenter` 和 `mouseleave` 事件
   - 监听弹窗自身的 `mouseenter` 和 `mouseleave` 事件

4. **位置计算**:
   - 根据目标元素的位置和弹窗尺寸计算显示位置
   - 考虑页面滚动，使用 `window.scrollY` 和 `window.scrollX`

5. **显示/隐藏**:
   - 使用 setTimeout 实现延迟显示/隐藏
   - 通过添加/移除 `.show` class 控制显示状态

### 浏览器兼容性

- ✅ Chrome 60+
- ✅ Firefox 55+
- ✅ Safari 11+
- ✅ Edge 79+
- ⚠️ IE 11 (需要 polyfill for `forEach` 等)

## 常见问题

### Q1: 二维码没有显示？

**A**: 检查以下几点：
1. 确保已引入 QRCode.js 库
2. 确保选择器正确匹配到目标元素
3. 检查浏览器控制台是否有错误
4. 确保 `url` 和 `selector` 参数都已提供

### Q2: 二维码位置不正确？

**A**: 
- 尝试调整 `position` 参数（`top`/`bottom`/`left`/`right`）
- 检查目标元素的 CSS 定位是否影响计算
- 如果页面有固定定位的元素，可能需要调整 z-index

### Q3: 如何修改二维码样式？

**A**: 
- 修改 `size` 参数改变二维码大小
- 通过覆盖 CSS 类自定义弹窗样式（见"样式定制"章节）
- 修改 `framework.js` 中的 HTML 结构来完全自定义

### Q4: 如何在其他页面使用？

**A**: 按照以下步骤：
1. 在 HTML 中添加触发元素，添加 class
2. 引入 QRCode.js 和 framework.js
3. 在页面 JS 中调用 `DBSyncerTheme.initQRCodePopover()`
4. 根据需要调整配置参数

### Q5: 如何禁止点击跳转，只显示二维码？

**A**: 
```javascript
// 方法 1: 移除 href
<span class="btn btn-primary qrcode-trigger">
    <i class="fa fa-wechat"></i> 联系客服
</span>

// 方法 2: 阻止默认行为
$('.qrcode-trigger').on('click', function(e) {
    e.preventDefault();
});
```

## 进阶用法

### 动态生成二维码

```javascript
// 根据不同按钮生成不同的二维码
$('.dynamic-qrcode').each(function() {
    const url = $(this).data('qrcode-url'); // 从 data 属性获取 URL
    
    DBSyncerTheme.initQRCodePopover({
        url: url,
        selector: '#' + this.id,
        size: 150,
        position: 'bottom'
    });
});
```

```html
<button id="btn1" class="dynamic-qrcode" data-qrcode-url="https://example.com/1">按钮 1</button>
<button id="btn2" class="dynamic-qrcode" data-qrcode-url="https://example.com/2">按钮 2</button>
```

### 响应式位置

```javascript
// 根据屏幕宽度决定位置
const isMobile = window.innerWidth < 768;

DBSyncerTheme.initQRCodePopover({
    url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776',
    selector: '.qrcode-trigger',
    size: isMobile ? 120 : 150,
    position: isMobile ? 'top' : 'bottom'
});
```

## 更新日志

### v1.0.0 (2025-10-30)
- ✅ 初始版本
- ✅ 支持基本的二维码悬浮显示
- ✅ 支持 4 个方向定位
- ✅ 支持延迟显示/隐藏
- ✅ 支持多个元素
- ✅ 平滑动画效果

## 许可证

MIT License

## 作者

DBSyncer Team

## 参考

- [QRCode.js](https://github.com/davidshimjs/qrcodejs)
- [企业微信](https://work.weixin.qq.com/)

