/**
 * 确认对话框组件
 * @param {Object} options - 配置选项
 *   - title: 对话框标题（与 titleHtml 二选一，或同时提供时 title 作为纯文本标题）
 *   - titleHtml: 自定义标题 HTML（不转义，替代默认 h3 标题）
 *   - titleExtra: 标题下方扩展 HTML（不转义，如统计摘要等）
 *   - message: 对话框内容信息
 *   - body: 对话框主体HTML内容（可选）
 *   - icon: 图标类型（info/warning/error/success，默认 info）
 *   - confirmText: 确认按钮文本（默认 '确定'）
 *   - cancelText: 取消按钮文本（默认 '取消'）
 *   - confirmType: 确认按钮类型（primary/success/warning/danger/info，默认 primary）
 *   - size: 对话框大小（normal/large/max，默认 normal）
 *   - position: 弹窗位置（top/center，默认 center）
 *   - showConfirm: 是否显示确认按钮（默认 true）
 *   - showCancel: 是否显示取消按钮（默认 true）
 *   - closeOnConfirm: 点击确认后是否关闭（默认 true）
 *   - closeOnCancel: 点击取消后是否关闭（默认 true）
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
        if (!options.title && !options.titleHtml) {
            console.warn('[showConfirm] 缺少必要参数: title 或 titleHtml');
            return;
        }

        // 默认配置
        const config = {
            title: options.title || '',
            titleHtml: options.titleHtml || '',
            titleExtra: options.titleExtra || '',
            message: options.message || '',
            body: options.body || '',
            icon: options.icon || 'info',
            confirmText: options.confirmText || '确定',
            cancelText: options.cancelText || '取消',
            confirmType: options.confirmType || 'primary',
            size: options.size || 'normal',
            position: options.position || 'center',
            showConfirm: options.showConfirm !== false,
            showCancel: options.showCancel !== false,
            closeOnConfirm: options.closeOnConfirm !== false,
            closeOnCancel: options.closeOnCancel !== false,
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

        const validPositions = ['top', 'center'];
        if (validPositions.indexOf(config.position) === -1) {
            config.position = 'center';
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
        overlay.className = 'confirm-overlay confirm-overlay-position-' + config.position;

        const dialog = document.createElement('div');
        dialog.id = dialogId;
        dialog.className = 'confirm-dialog size-' + config.size;

        // 构建头部HTML
        let headerHTML = '';
        if (config.title || config.titleHtml || config.message || config.titleExtra) {
            let titleHTML = '';
            if (config.titleHtml) {
                titleHTML = `<div class="confirm-title-html">${config.titleHtml}</div>`;
            } else if (config.title) {
                titleHTML = `<h3 class="confirm-title">${escapeHtml(config.title)}</h3>`;
            }
            headerHTML = `
                <div class="confirm-header">
                    <div class="confirm-icon icon-${config.icon}">
                        ${getIconHTML()}
                    </div>
                    <div class="confirm-content-wrapper">
                        ${titleHTML}
                        ${config.titleExtra ? `<div class="confirm-title-extra">${config.titleExtra}</div>` : ''}
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

        let footerHTML = '';
        if (config.showConfirm || config.showCancel) {
            let footerButtons = '';
            if (config.showConfirm) {
                footerButtons += `<button class="btn confirm-btn confirm-btn-confirm confirm-btn-type-${config.confirmType}">${escapeHtml(config.confirmText)}</button>`;
            }
            if (config.showCancel) {
                footerButtons += `<button class="btn confirm-btn confirm-btn-cancel">${escapeHtml(config.cancelText)}</button>`;
            }
            footerHTML = `<div class="confirm-footer">${footerButtons}</div>`;
        }

        dialog.innerHTML = `
            ${headerHTML}
            ${bodyHTML}
            ${footerHTML}
        `;

        overlay.appendChild(dialog);
        document.body.appendChild(overlay);

        // 获取按钮
        const confirmBtn = dialog.querySelector('.confirm-btn-confirm');
        const cancelBtn = dialog.querySelector('.confirm-btn-cancel');

        let closed = false;

        function removeKeydownListener() {
            document.removeEventListener('keydown', handleKeydown, true);
        }

        // 关闭对话框
        function closeConfirm() {
            if (closed) {
                return;
            }
            closed = true;
            removeKeydownListener();
            dialog.classList.add('hidden');
            overlay.classList.add('hidden');
            setTimeout(function() {
                if (overlay.parentNode) {
                    overlay.parentNode.removeChild(overlay);
                }
            }, 200);
        }

        function triggerConfirm() {
            if (typeof config.onConfirm === 'function') {
                config.onConfirm();
            }
            if (config.closeOnConfirm) {
                closeConfirm();
            }
        }

        function triggerCancel() {
            if (typeof config.onCancel === 'function') {
                config.onCancel();
            }
            if (config.closeOnCancel) {
                closeConfirm();
            }
        }

        // 确认按钮事件
        if (confirmBtn) {
            confirmBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                triggerConfirm();
            });
        }

        // 取消按钮事件
        if (cancelBtn) {
            cancelBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                triggerCancel();
            });
        }

        // 点击非对话框区域关闭
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) {
                e.preventDefault();
                triggerCancel();
            }
        });

        // Enter 确认、Esc 取消（捕获阶段，避免触发底层按钮的默认行为）
        function handleKeydown(e) {
            if (e.key === 'Enter') {
                if (e.isComposing) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                if (cancelBtn && document.activeElement === cancelBtn) {
                    triggerCancel();
                } else if (confirmBtn) {
                    triggerConfirm();
                } else if (cancelBtn) {
                    triggerCancel();
                }
            } else if (e.key === 'Escape') {
                e.preventDefault();
                e.stopPropagation();
                triggerCancel();
            }
        }
        document.addEventListener('keydown', handleKeydown, true);

        // 避免回车再次触发打开弹窗的按钮（如删除）
        if (document.activeElement && typeof document.activeElement.blur === 'function') {
            document.activeElement.blur();
        }
        if (confirmBtn) {
            confirmBtn.focus();
        } else if (cancelBtn) {
            cancelBtn.focus();
        }

        return {
            close: closeConfirm,
            getDialog: function() { return dialog; },
            getOverlay: function() { return overlay; }
        };
    }
    
    // 导出到全局
    window.showConfirm = showConfirm;
    
})(window);