/**
 * 通知/Toast 组件
 */
(function(window) {
    'use strict';
    
    function ensureToastContainer() {
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }
        return container;
    }

    function notify(message, type, options) {
        if (typeof message === 'object') {
            options = message;
            message = options.message;
        }
        options = options || {};
        var cfg = {
            message: message || '',
            title: options.title || '',
            type: type || options.type || 'info',
            duration: options.duration || 3200,
            icon: options.icon
        };

        var container = ensureToastContainer();
        var toast = document.createElement('div');
        toast.className = 'toast toast-' + cfg.type;

        var iconHtml = cfg.icon;
        if (!iconHtml) {
            switch (cfg.type) {
                case 'success': iconHtml = '<i class="fa fa-check"></i>'; break;
                case 'danger': iconHtml = '<i class="fa fa-times"></i>'; break;
                case 'warning': iconHtml = '<i class="fa fa-exclamation"></i>'; break;
                default: iconHtml = '<i class="fa fa-info"></i>';
            }
        }

        toast.innerHTML = '' +
            '<div class="toast-icon">' + iconHtml + '</div>' +
            '<div class="toast-content">' +
                (cfg.title ? '<div class="toast-title">' + cfg.title + '</div>' : '') +
                '<div class="toast-message">' + cfg.message + '</div>' +
            '</div>' +
            '<button type="button" class="toast-close" aria-label="关闭">&times;</button>' +
            '<div class="toast-progress"><div class="toast-progress-bar"></div></div>';

        var closeBtn = toast.querySelector('.toast-close');
        closeBtn.addEventListener('click', function (e) {
            e.stopPropagation();
            removeToast(toast);
        });

        container.appendChild(toast);

        var progress = toast.querySelector('.toast-progress-bar');
        if (progress) {
            progress.style.animationDuration = cfg.duration + 'ms';
        }

        var timer = setTimeout(function () {
            removeToast(toast);
        }, cfg.duration);

        toast.addEventListener('mouseenter', function () {
            clearTimeout(timer);
            if (progress) {
                progress.style.animationPlayState = 'paused';
            }
        });

        toast.addEventListener('mouseleave', function () {
            if (progress) {
                progress.style.animationPlayState = 'running';
            }
            timer = setTimeout(function () {
                removeToast(toast);
            }, cfg.duration / 2);
        });
    }

    function removeToast(toast) {
        if (!toast) { return; }
        toast.style.animation = 'toast-exit 0.2s ease forwards';
        setTimeout(function () {
            if (toast && toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 180);
    }

    function bootGrowl(message, type, duration) {
        notify({ message: message, type: type || 'info', duration: duration || 3200 });
    }
    
    // 导出到全局
    window.notify = notify;
    window.removeToast = removeToast;
    window.bootGrowl = bootGrowl;
    
})(window);