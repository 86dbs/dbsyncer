// ******************* 初始化 *****************************
const $location = (window.location + '').split('/');
const $path = document.location.pathname;
const $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/") + 1);
const $mainContent = $('#mainContent');

// 工具函数
function showLoading() {
    $mainContent.html('<div class="loading"><div class="loading-spinner"></div>加载中...</div>');
}
function hideLoading() {
    $mainContent.find('.loading').remove();
}

// HTML转义
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatDate(time) {
    const date = new Date(time);
    const YY = date.getFullYear() + '-';
    const MM = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
    const DD = (date.getDate() < 10 ? '0' + (date.getDate()) : date.getDate());
    const hh = (date.getHours() < 10 ? '0' + date.getHours() : date.getHours()) + ':';
    const mm = (date.getMinutes() < 10 ? '0' + date.getMinutes() : date.getMinutes()) + ':';
    const ss = (date.getSeconds() < 10 ? '0' + date.getSeconds() : date.getSeconds());
    return YY + MM + DD + " " + hh + mm + ss;
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
                bootGrowl('页面加载失败，请稍后重试', "danger");
            }
        });
    }
}

// 异常请求
function doRequest(action, data) {
    hideLoading();
    // 异常请求：302
    if (!(data instanceof Object)) {
        bootGrowl("会话过期, 3秒后将访问登录主页...", "danger");
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
    bootGrowl("访问异常，请刷新或重试.", "danger");
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

function initSearch(searchWrapperId, callback) {
    const wrapper = document.getElementById(searchWrapperId);
    const searchInput = wrapper.querySelector('.search-input');
    const searchClear = wrapper.querySelector('.search-clear');

    if (!searchInput || !searchClear || searchInput.dataset.selectBound === 'true') {
        return null;
    }

    // 更新搜索状态（显示/隐藏清除按钮）
    function updateSearchState(keyword) {
        if (keyword && keyword.trim() !== '') {
            searchClear.classList.add('active');
        } else {
            searchClear.classList.remove('active');
        }
    }

    // 清除搜索的内部实现
    function clearSearch() {
        searchInput.value = '';
        searchInput.focus();
        updateSearchState('');
        if (callback) {
            callback(searchInput.value);
        }
    }

    // 绑定事件
    searchInput.addEventListener('input', function(e) {
        const value = e.target.value;
        // 立即更新清除按钮显示状态
        updateSearchState(value);
    });

    // 清除搜索
    searchClear.addEventListener('click', clearSearch);

    // 回车键搜索
    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            updateSearchState(e.target.value);
            if (callback) {
                callback(e.target.value);
            }
        }
    });

    // 避免重复绑定
    searchInput.dataset.selectBound = 'true';

    // 初始化状态
    updateSearchState(searchInput.value || '');

    // 返回包含 input 实例和方法的对象
    return {
        // input 元素实例
        input: searchInput,
        // 模拟点击清除按钮
        clear: function() {
            clearSearch();
        },
        // 设置搜索值
        setValue: function(value) {
            searchInput.value = value || '';
            updateSearchState(searchInput.value);
            if (callback) {
                callback(searchInput.value);
            }
        },
        // 获取搜索值
        getValue: function() {
            return searchInput.value;
        },
        // 触发搜索（模拟回车）
        search: function() {
            updateSearchState(searchInput.value);
            if (callback) {
                callback(searchInput.value);
            }
        }
    };
}

function initSelect(selector){
    selector.dbSelect({
        type: 'single'
    });
}

function initSwitch(dbSwitch, callback) {
    const $input = dbSwitch;
    $input.attr('role', 'switch');
    $input.attr('aria-checked', $input.is(':checked'));
    $input.on('change', function () {
        $input.attr('aria-checked', this.checked);
        callback(this.checked);
    });

    return {
        isChecked: function () {
            return $input.is(':checked');
        }
    }
}

// 注销
function logout() {
    // 基础用法
    showConfirm({
        title: '确定要注销？',
        message: '确定后将跳转至登录页面',
        icon: 'info',
        size: 'large',
        confirmType: 'info',
        onConfirm: function () {
            doPoster("/logout", null, function (data) {
                location.href = $basePath;
            });
        }
    });
}

// 刷新登录人信息
function refreshLoginUser() {
    // 刷新登录用户
    doGetter("/user/getUserInfo.json", {}, function (data) {
        if (data.success === true) {
            $("#currentUser").text(data.resultValue.nickname);
        }
    });
}

// 刷新授权信息
function refreshLicense() {
    // 刷新授权信息
    doGetter("/license/query.json", {}, function (data) {
        if (data.success === true) {
            // 社区版
            if (isBlank(data.resultValue.key)) {
                return;
            }
            $("#licenseInfo").show();
            // 专业版
            const licenseInfo = data.resultValue;
            const $content = $("#effectiveContent");
            const $effectiveTime = licenseInfo.effectiveTime;
            if ($effectiveTime <= 0) {
                $content.text('未激活');
                $content.addClass('text-warning');
                return;
            }
            const $currentTime = licenseInfo.currentTime;
            const $10days = 864000000;
            // 有效期内
            if ($currentTime < $effectiveTime && $effectiveTime - $10days > $currentTime) {
                $("#licenseCheck").removeClass("hidden");
            }
            // 即将过期
            else if ($currentTime < $effectiveTime && $effectiveTime - $10days <= $currentTime) {
                $("#licenseRemind").removeClass("hidden");
            }
            // 已过期
            else if ($currentTime > $effectiveTime) {
                $("#licenseWarning").removeClass("hidden");
            }
            $content.text(licenseInfo.effectiveContent);
        }
    });
}

// 同步任务列表自动刷新定时器
let mappingListAutoRefreshTimer = null;

// 自动刷新间隔（毫秒），默认5秒
const MAPPING_LIST_REFRESH_INTERVAL = 5000;

$(function () {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/index');
    };
    window.validateForm = validateForm;

    refreshLoginUser();
    refreshLicense();
    // 初始化版权信息
    doGetter("/index/version.json", {}, function (data) {
        if (data.success === true) {
            // 获取底部版权信息
            $("#copyRight").html(data.resultValue.appCopyRight);
            settings.watermark_txt = data.resultValue.watermark;
            watermark();
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
    const $dropdownBtn = $(".dropdown button");
    const $dropdownMenu = $(".dropdown .dropdown-menu");
    $dropdownBtn.on('click', function (){
        event.stopPropagation();
        // 只切换当前下拉菜单，不影响其他下拉菜单
        const $currentDropdown = $(this).closest('.dropdown');
        const $currentMenu = $currentDropdown.find('.dropdown-menu');
        // 关闭其他下拉菜单
        $dropdown.not($currentDropdown).removeClass("open");
        $dropdownMenu.not($currentMenu).addClass("hidden");
        // 切换当前下拉菜单
        $currentDropdown.toggleClass("open");
        $currentMenu.toggleClass("hidden");
    });
    // 只有点击下拉菜单按钮或菜单项外才关闭
    $(document).on('click', function(event) {
        if (!$(event.target).closest('.dropdown').length) {
            $dropdown.removeClass("open");
            $dropdownMenu.addClass("hidden");
        }
    });
});