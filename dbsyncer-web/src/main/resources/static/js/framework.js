// ******************* 初始化 *****************************
const $location = (window.location + '').split('/');
const $path = document.location.pathname;
const $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/") + 1);
const $mainContent = $('#mainContent');

// 工具函数
function showLoading() {
    $mainContent.html('<div class="dbsyncer-loading"><div class="dbsyncer-loading-spinner"></div>加载中...</div>');
}
function hideLoading() {
    $mainContent.find('.dbsyncer-loading').remove();
}
function showEmpty(element, message) {
    $(element).html('<div class="dbsyncer-empty"><div class="dbsyncer-empty-icon"><i class="fa fa-inbox"></i></div><div class="dbsyncer-empty-text">' + (message || '暂无数据') + '</div></div>');
}

/**
 * 判断字符串是否为空串
 * @eg undefined true
 * @eg null true
 * @eg '' true
 * @eg ' ' true
 * @eg '1' false
 * @return Boolean
 */
function isBlank(str) {
    return str === undefined || str === null || str === false || str.length === 0;
}

/**
 * 按照指定分隔符切分字符串
 *
 * @param str 带切分字符
 * @param delimiter 分隔符
 * @return Array
 */
function splitStrByDelimiter(str, delimiter) {
    return isBlank(str) ? [] : str.split(delimiter);
}

// ******************* 水印 ***************************
window.onresize = function () {
    watermark();
}
window.onscroll = function () {
    watermark();
}
//水印样式默认设置
const settings = {
    watermark_txt: "",
    watermark_x: 50,//水印起始位置x轴坐标
    watermark_y: 55,//水印起始位置Y轴坐标
    watermark_rows: 2000,//水印行数
    watermark_cols: 2000,//水印列数
    watermark_x_space: 70,//水印x轴间隔
    watermark_y_space: 30,//水印y轴间隔
    watermark_color: '#aaaaaa',//水印字体颜色
    watermark_alpha: 0.2,//水印透明度
    watermark_fontsize: '15px',//水印字体大小
    watermark_font: '微软雅黑',//水印字体
    watermark_width: 210,//水印宽度
    watermark_height: 80,//水印长度
    watermark_angle: 15//水印倾斜度数
};
let timestampWatermark;

function watermark() {
    const now = Date.now();
    if (timestampWatermark != null && now - timestampWatermark < 200) {
        return;
    }
    if (isBlank(settings.watermark_txt)) {
        return;
    }
    timestampWatermark = now;
    $(".dbsyncer_mask").remove();

    const water = document.body;
    //获取页面最大宽度
    const page_width = Math.max(water.scrollWidth, water.clientWidth);
    //获取页面最大高度
    const page_height = Math.max(water.scrollHeight, water.clientHeight);
    //水印数量自适应元素区域尺寸
    settings.watermark_cols = Math.ceil(page_width / (settings.watermark_x_space + settings.watermark_width));
    settings.watermark_rows = Math.ceil(page_height / (settings.watermark_y_space + settings.watermark_height));
    let x;
    let y;
    for (let i = 0; i < settings.watermark_rows; i++) {
        y = settings.watermark_y + (settings.watermark_y_space + settings.watermark_height) * i;
        for (let j = 0; j < settings.watermark_cols; j++) {
            x = settings.watermark_x + (settings.watermark_width + settings.watermark_x_space) * j;
            let mask_div = document.createElement('div');
            mask_div.className = 'dbsyncer_mask';
            mask_div.innerHTML = (settings.watermark_txt);
            mask_div.style.webkitTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.MozTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.msTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.OTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.transform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.position = "absolute";
            mask_div.style.left = x + 'px';
            mask_div.style.top = y + 'px';
            mask_div.style.overflow = "hidden";
            mask_div.style.pointerEvents = 'none';
            mask_div.style.opacity = settings.watermark_alpha;
            mask_div.style.fontSize = settings.watermark_fontsize;
            mask_div.style.fontFamily = settings.watermark_font;
            mask_div.style.color = settings.watermark_color;
            mask_div.style.textAlign = "center";
            mask_div.style.width = settings.watermark_width + 'px';
            mask_div.style.height = settings.watermark_height + 'px';
            mask_div.style.display = "block";
            water.appendChild(mask_div);
        }
    }
}

// ******************* 扩展JS表单方法 ***************************
$.fn.serializeJson = function () {
    var o = {};
    var a = this.serializeArray();
    $.each(a, function () {
        if (o[this.name] !== undefined) {
            if (!o[this.name].push) {
                o[this.name] = [o[this.name]];
            }
            o[this.name].push(this.value || '');
        } else {
            o[this.name] = this.value || '';
        }
    });
    return o;
};

// 全局加载页面
function doLoader(url) {
    // 使用统一的内容区域
    if ($mainContent.length) {
        // 显示加载状态
        showLoading();

        // 加载页面内容
        $mainContent.load($basePath + url, function (response, status, xhr) {
            hideLoading();
            if (status !== 'success') {
                alert('页面加载失败，请稍后重试');
            }
        });
    }
}

// 异常请求
function doRequest(action, data) {
    hideLoading();
    // 异常请求：302
    if (!(data instanceof Object)) {
        alert("会话过期, 3秒后将访问登录主页...");
        setTimeout(function () {
            location.href = $basePath;
        }, 3000);
    } else {
        action(data);
    }
}

// 异常响应
function doErrorResponse(xhr, status, info) {
    hideLoading();
    alert("访问异常，请刷新或重试.");
}

// 全局Ajax post
function doPoster(url, params, action) {
    $.post($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

// 全局Ajax get
function doGetter(url, params, action) {
    $.get($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

$(function () {
    // 导出到全局
    window.DBSyncerTheme = {
        showLoading: showLoading,
        hideLoading: hideLoading,
        showEmpty: showEmpty,
        validateForm: validateForm,
        notify: notify,
        enhanceSelects: enhanceSelects
    };

    // // 刷新登录用户
    // refreshLoginUser();
    // // 刷新授权信息
    // refreshLicenseInfo();

    // 初始化版权信息
    doGetter("/index/version.json", {}, function (data) {
        if (data.success == true) {
            // 获取底部版权信息
            $("#copyRight").html(data.resultValue.appCopyRight);
            settings.watermark_txt = data.resultValue.watermark;
            watermark();
        }
    });

    // 修改登录用户
    $("#edit_personal").click(function () {
        doLoader("/user/page/edit?username=" + $(this).attr("username"));
    });

    // 注销
    $("#nav_logout").click(function () {
        if (confirm('确定要注销吗？')) {
            doPoster("/logout", null, function (data) {
                location.href = $basePath;
            });
        }
    });

    // 新导航链接点击事件
    $('.sidebar-item[url]').on('click', function(e) {
        e.preventDefault();
        // 更新活动状态
        $('.sidebar-item').removeClass('active');
        $(this).addClass('active');
        // 加载页面
        doLoader($(this).attr('url'));
    });

    // 下拉菜单
    const $dropdown = $(".dropdown");
    const $$dropdownBtn = $(".dropdown button");
    const $$dropdownMenu = $(".dropdown .dropdown-menu");
    $$dropdownBtn.on('click', function (){
        event.stopPropagation();
        $dropdown.toggleClass("open");
        $$dropdownMenu.toggleClass("hidden");
    });
    $(document).on('click', function(event) {
        $dropdown.toggleClass("open");
        $$dropdownMenu.addClass("hidden");
    });
    enhanceSelects();
});

function validateForm($form) {
    if (!$form || !$form.length) { return true; }

    var isValid = true;
    $form.find('.form-error-msg').remove();
    $form.find('.is-invalid').removeClass('is-invalid');

    $form.find('[required]').each(function(){
        var $field = $(this);
        if ($field.is(':disabled')) { return; }
        var value = $.trim($field.val());
        var invalid = false;

        if ($field.is(':checkbox') || $field.is(':radio')) {
            invalid = !$field.is(':checked');
        } else if ($field.is('select')) {
            invalid = value === '' || value === null;
        } else {
            invalid = value.length === 0;
        }

        if (invalid) {
            isValid = false;
            var $container = $field.closest('.form-control-area');
            if ($container.length === 0) {
                $container = $field.parent();
            }
            var labelText = '';
            var $label = $field.closest('.form-item').find('.form-label').first();
            if ($label.length) {
                labelText = $label.text().replace('*', '').trim();
            } else if ($field.attr('placeholder')) {
                labelText = $field.attr('placeholder');
            } else {
                labelText = '该字段';
            }

            $field.addClass('is-invalid');
            if ($container.length) {
                $('<div class="form-error-msg"><i class="fa fa-exclamation-circle"></i>' + labelText + '不能为空</div>').appendTo($container);
            }
        }
    });

    return isValid;
}

var dbsyncerSelects = [];
var dbsyncerSelectEventsBound = false;

function enhanceSelects(root) {
    var scope = root || document;
    var nodes = scope.querySelectorAll('select.select-control:not([data-dbs-enhanced])');
    if (!nodes.length) { return; }

    nodes.forEach(function (select) {
        if (select.dataset.dbsEnhanced === 'true') { return; }
        select.dataset.dbsEnhanced = 'true';

        var container = document.createElement('div');
        container.className = 'dbsyncer-select';

        var trigger = document.createElement('button');
        trigger.type = 'button';
        trigger.className = 'dbsyncer-select-trigger';

        var textSpan = document.createElement('span');
        textSpan.className = 'dbsyncer-select-text';
        trigger.appendChild(textSpan);

        var arrow = document.createElement('span');
        arrow.className = 'dbsyncer-select-arrow';
        trigger.appendChild(arrow);

        var panel = document.createElement('div');
        panel.className = 'dbsyncer-select-panel';

        var parent = select.parentNode;
        parent.insertBefore(container, select);
        container.appendChild(trigger);
        container.appendChild(panel);
        container.appendChild(select);
        select.classList.add('dbsyncer-select-original');

        function buildOptions() {
            panel.innerHTML = '';
            Array.from(select.options).forEach(function (opt) {
                var optionBtn = document.createElement('button');
                optionBtn.type = 'button';
                optionBtn.className = 'dbsyncer-select-option' + (opt.selected ? ' active' : '');
                optionBtn.textContent = opt.textContent;
                optionBtn.dataset.value = opt.value;
                optionBtn.addEventListener('click', function (e) {
                    e.stopPropagation();
                    Array.from(panel.querySelectorAll('.dbsyncer-select-option')).forEach(function (btn) {
                        btn.classList.remove('active');
                    });
                    optionBtn.classList.add('active');
                    select.value = opt.value;
                    updateFromSelect();
                    select.dispatchEvent(new Event('change', { bubbles: true }));
                    closeAllSelects();
                });
                panel.appendChild(optionBtn);
            });
        }

        function updateFromSelect() {
            var selectedOption = select.options[select.selectedIndex];
            textSpan.textContent = selectedOption ? selectedOption.text : (select.getAttribute('placeholder') || '请选择');
            Array.from(panel.querySelectorAll('.dbsyncer-select-option')).forEach(function (btn) {
                btn.classList.toggle('active', btn.dataset.value === select.value);
            });
            if (select.disabled) {
                container.classList.add('disabled');
                trigger.disabled = true;
            } else {
                container.classList.remove('disabled');
                trigger.disabled = false;
            }
        }

        buildOptions();
        updateFromSelect();

        trigger.addEventListener('click', function (e) {
            e.stopPropagation();
            if (container.classList.contains('disabled')) { return; }
            var isOpen = container.classList.contains('open');
            closeAllSelects(container);
            if (!isOpen) {
                container.classList.add('open');
            } else {
                container.classList.remove('open');
            }
        });

        panel.addEventListener('click', function (e) { e.stopPropagation(); });

        select.addEventListener('change', function () {
            updateFromSelect();
        });

        dbsyncerSelects.push(container);
    });

    if (!dbsyncerSelectEventsBound) {
        document.addEventListener('click', function () {
            closeAllSelects();
        });
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') {
                closeAllSelects();
            }
        });
        dbsyncerSelectEventsBound = true;
    }
}

function closeAllSelects(except) {
    dbsyncerSelects.forEach(function (container) {
        if (container !== except) {
            container.classList.remove('open');
        }
    });
}

function ensureToastContainer() {
    var container = document.querySelector('.dbsyncer-toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'dbsyncer-toast-container';
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
    toast.className = 'dbsyncer-toast dbsyncer-toast-' + cfg.type;

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
        '<div class="dbsyncer-toast-icon">' + iconHtml + '</div>' +
        '<div class="dbsyncer-toast-content">' +
            (cfg.title ? '<div class="dbsyncer-toast-title">' + cfg.title + '</div>' : '') +
            '<div class="dbsyncer-toast-message">' + cfg.message + '</div>' +
        '</div>' +
        '<button type="button" class="dbsyncer-toast-close" aria-label="关闭">&times;</button>' +
        '<div class="dbsyncer-toast-progress"><div class="dbsyncer-toast-progress-bar"></div></div>';

    var closeBtn = toast.querySelector('.dbsyncer-toast-close');
    closeBtn.addEventListener('click', function (e) {
        e.stopPropagation();
        removeToast(toast);
    });

    container.appendChild(toast);

    var progress = toast.querySelector('.dbsyncer-toast-progress-bar');
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

window.bootGrowl = function (message, type, duration) {
    notify({ message: message, type: type || 'info', duration: duration || 3200 });
};

window.initSelectIndex = function ($select) {
    if (!$select || !$select.length) { return $select; }
    if (window.DBSyncerTheme) {
        DBSyncerTheme.enhanceSelects($select[0].parentNode || document);
    }
    return $select;
};