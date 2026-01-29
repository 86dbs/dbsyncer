/**
 * 确认对话框组件
 * @param {Object} options - 配置选项
 *   - title: 对话框标题（必需）
 *   - message: 对话框内容信息
 *   - body: 对话框主体HTML内容（可选）
 *   - icon: 图标类型（info/warning/error/success，默认 info）
 *   - confirmText: 确认按钮文本（默认 '确定'）
 *   - cancelText: 取消按钮文本（默认 '取消'）
 *   - confirmType: 确认按钮类型（primary/success/warning/danger/info，默认 primary）
 *   - size: 对话框大小（normal/large/max，默认 normal）
 *   - onConfirm: 确认回调函数
 *   - onCancel: 取消回调函数（可选）
 */
(function(window) {
    'use strict';
    
    // HTML转义函数
    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    function showConfirm(options) {
        options = options || {};
        
        // 参数验证
        if (!options.title) {
            console.warn('[showConfirm] 缺少必要参数: title');
            return;
        }

        // 默认配置
        const config = {
            title: options.title || '',
            message: options.message || '',
            body: options.body || '',
            icon: options.icon || 'info',
            confirmText: options.confirmText || '确定',
            cancelText: options.cancelText || '取消',
            confirmType: options.confirmType || 'primary',
            size: options.size || 'normal',
            onConfirm: options.onConfirm || function() {},
            onCancel: options.onCancel || function() {}
        };

        // 验证icon类型
        const validIcons = ['info', 'warning', 'error', 'success'];
        if (validIcons.indexOf(config.icon) === -1) {
            config.icon = 'info';
        }

        // 验证size
        const validSizes = ['normal', 'large', 'max'];
        if (validSizes.indexOf(config.size) === -1) {
            config.size = 'normal';
        }

        // 验证确认按钮类型
        const validTypes = ['primary', 'success', 'warning', 'danger', 'info'];
        if (validTypes.indexOf(config.confirmType) === -1) {
            config.confirmType = 'primary';
        }

        // 获取icon HTML
        function getIconHTML() {
            const iconMap = {
                'info': '<i class="fa fa-info-circle"></i>',
                'warning': '<i class="fa fa-exclamation-triangle"></i>',
                'error': '<i class="fa fa-times-circle"></i>',
                'success': '<i class="fa fa-check-circle"></i>'
            };
            return iconMap[config.icon] || iconMap['info'];
        }

        // 创建HTML
        const overlayId = 'confirm-overlay-' + Date.now();
        const dialogId = 'confirm-dialog-' + Date.now();
        
        const overlay = document.createElement('div');
        overlay.id = overlayId;
        overlay.className = 'confirm-overlay';

        const dialog = document.createElement('div');
        dialog.id = dialogId;
        dialog.className = 'confirm-dialog size-' + config.size;

        // 构建头部HTML
        let headerHTML = '';
        if (config.title || config.message) {
            headerHTML = `
                <div class="confirm-header">
                    <div class="confirm-icon icon-${config.icon}">
                        ${getIconHTML()}
                    </div>
                    <div class="confirm-content-wrapper">
                        ${config.title ? `<h3 class="confirm-title">${escapeHtml(config.title)}</h3>` : ''}
                        ${config.message ? `<p class="confirm-message">${escapeHtml(config.message)}</p>` : ''}
                    </div>
                </div>
            `;
        }

        // 构建主体HTML
        let bodyHTML = '';
        if (config.body) {
            bodyHTML = `<div class="confirm-body">${config.body}</div>`;
        }

        dialog.innerHTML = `
            ${headerHTML}
            ${bodyHTML}
            <div class="confirm-footer">
                <button id="confirm-btn-confirm-${Date.now()}" class="btn confirm-btn confirm-btn-type-${config.confirmType}">
                    ${escapeHtml(config.confirmText)}
                </button>
                <button id="confirm-btn-cancel-${Date.now()}" class="btn confirm-btn confirm-btn-cancel">
                    ${escapeHtml(config.cancelText)}
                </button>
            </div>
        `;

        overlay.appendChild(dialog);
        document.body.appendChild(overlay);

        // 获取按钮
        const confirmBtn = dialog.querySelector('button:first-child');
        const cancelBtn = dialog.querySelector('button:last-child');

        // 关闭对话框
        function closeConfirm() {
            dialog.classList.add('hidden');
            overlay.classList.add('hidden');
            setTimeout(function() {
                if (overlay.parentNode) {
                    overlay.parentNode.removeChild(overlay);
                }
            }, 200);
        }

        // 确认按钮事件
        confirmBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            closeConfirm();
            if (typeof config.onConfirm === 'function') {
                config.onConfirm();
            }
        });

        // 取消按钮事件
        cancelBtn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            closeConfirm();
            if (typeof config.onCancel === 'function') {
                config.onCancel();
            }
        });

        // 点击非对话框区域关闭
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) {
                e.preventDefault();
                closeConfirm();
                if (typeof config.onCancel === 'function') {
                    config.onCancel();
                }
            }
        });

        // ESC键关闭（可选）
        function handleEsc(e) {
            if (e.key === 'Escape') {
                closeConfirm();
                if (typeof config.onCancel === 'function') {
                    config.onCancel();
                }
                document.removeEventListener('keydown', handleEsc);
            }
        }
        document.addEventListener('keydown', handleEsc);

        return {
            close: closeConfirm,
            getDialog: function() { return dialog; },
            getOverlay: function() { return overlay; }
        };
    }
    
    // 导出到全局
    window.showConfirm = showConfirm;
    
})(window);