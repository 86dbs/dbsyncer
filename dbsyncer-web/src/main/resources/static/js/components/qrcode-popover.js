/**
 * 初始化二维码悬浮提示
 * @param {Object} options - 配置选项
 * @param {string} options.url - 二维码链接地址
 * @param {string} options.selector - 目标元素选择器（多个用逗号分隔）
 * @param {number} options.size - 二维码大小，默认 150
 * @param {string} options.position - 位置，默认 'bottom'（bottom/top/left/right）
 */
(function(window) {
    'use strict';
    
    function initQRCodePopover(options) {
        const config = {
            url: options.url || '',
            selector: options.selector || '',
            size: options.size || 150,
            position: options.position || 'bottom'
        };

        if (!config.url || !config.selector) {
            console.warn('initQRCodePopover: url and selector are required');
            return;
        }

        // 创建隐藏的二维码容器
        let qrcodeContainer = document.getElementById('qrcode-temp-container');
        if (!qrcodeContainer) {
            qrcodeContainer = document.createElement('div');
            qrcodeContainer.id = 'qrcode-temp-container';
            qrcodeContainer.style.cssText = 'position: absolute; left: -9999px; top: -9999px;';
            document.body.appendChild(qrcodeContainer);
        }

        // 生成二维码
        qrcodeContainer.innerHTML = ''; // 清空
        if (typeof QRCode !== 'undefined') {
            new QRCode(qrcodeContainer, {
                text: config.url,
                width: config.size,
                height: config.size,
                colorDark: '#000000',
                colorLight: '#ffffff',
                correctLevel: QRCode.CorrectLevel.H
            });
        } else {
            console.error('QRCode library not found');
            return;
        }

        // 等待二维码生成
        setTimeout(function() {
            const qrcodeImg = qrcodeContainer.querySelector('img');
            if (!qrcodeImg) {
                console.error('QRCode image not generated');
                return;
            }

            const qrcodeDataUrl = qrcodeImg.src;

            // 为所有匹配的元素添加悬浮效果
            const elements = document.querySelectorAll(config.selector);
            elements.forEach(function(element) {
                // 创建悬浮提示
                const popover = document.createElement('div');
                popover.className = 'qrcode-popover';
                popover.innerHTML = `
                    <div class="qrcode-arrow"></div>
                    <div class="qrcode-inner">
                        <img src="${qrcodeDataUrl}" alt="微信扫码" />
                        <div class="qrcode-text text-center text-success">
                            <i class="fa fa-wechat"></i> 微信扫码联系
                        </div>
                    </div>
                `;
                document.body.appendChild(popover);

                let showTimer = null;
                let hideTimer = null;

                // 显示二维码
                function showQRCode() {
                    clearTimeout(hideTimer);
                    showTimer = setTimeout(function() {
                        const rect = element.getBoundingClientRect();
                        const popoverRect = popover.getBoundingClientRect();
                        
                        let top, left;
                        popover.className = 'qrcode-popover qrcode-' + config.position;

                        switch (config.position) {
                            case 'top':
                                top = rect.top - popoverRect.height - 10;
                                left = rect.left + (rect.width - popoverRect.width) / 2;
                                break;
                            case 'left':
                                top = rect.top + (rect.height - popoverRect.height) / 2;
                                left = rect.left - popoverRect.width - 10;
                                break;
                            case 'right':
                                top = rect.top + (rect.height - popoverRect.height) / 2;
                                left = rect.right + 10;
                                break;
                            case 'bottom':
                            default:
                                top = rect.bottom + 10;
                                left = rect.left + (rect.width - popoverRect.width) / 2;
                                break;
                        }

                        popover.style.top = top + window.scrollY + 'px';
                        popover.style.left = left + window.scrollX + 'px';
                        popover.classList.add('show');
                    }, 300); // 延迟 300ms 显示
                }

                // 隐藏二维码
                function hideQRCode() {
                    clearTimeout(showTimer);
                    hideTimer = setTimeout(function() {
                        popover.classList.remove('show');
                    }, 200); // 延迟 200ms 隐藏
                }

                // 绑定事件
                element.addEventListener('mouseenter', showQRCode);
                element.addEventListener('mouseleave', hideQRCode);
                popover.addEventListener('mouseenter', function() {
                    clearTimeout(hideTimer);
                });
                popover.addEventListener('mouseleave', hideQRCode);
            });
        }, 500);
    }
    
    // 导出到全局
    window.initQRCodePopover = initQRCodePopover;
    
})(window);

