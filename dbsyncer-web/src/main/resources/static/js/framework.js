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
 * 计算相对时间文本
 * @param {string|number} time - 时间字符串或时间戳
 * @returns {string} 相对时间文本，如：30秒前、1分钟前、2小时前、昨天、2天前、3个月前、N个月前
 */
function formatRelativeTime(time) {
    if (!time) {
        return '';
    }
    
    const now = new Date();
    const targetTime = new Date(time);
    
    // 如果时间无效，返回空字符串
    if (isNaN(targetTime.getTime())) {
        return '';
    }
    
    const diffMs = now.getTime() - targetTime.getTime();
    const diffSeconds = Math.floor(diffMs / 1000);
    const diffMinutes = Math.floor(diffSeconds / 60);
    const diffHours = Math.floor(diffMinutes / 60);
    const diffDays = Math.floor(diffHours / 24);
    
    // 小于1分钟：显示秒数
    if (diffSeconds < 60) {
        return diffSeconds <= 0 ? '刚刚' : diffSeconds + '秒前';
    }
    
    // 小于1小时：显示分钟数
    if (diffMinutes < 60) {
        return diffMinutes + '分钟前';
    }
    
    // 小于24小时：显示小时数
    if (diffHours < 24) {
        return diffHours + '小时前';
    }
    
    // 判断是否是昨天（同一天）
    const nowDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const targetDate = new Date(targetTime.getFullYear(), targetTime.getMonth(), targetTime.getDate());
    const daysDiff = Math.floor((nowDate.getTime() - targetDate.getTime()) / (1000 * 60 * 60 * 24));
    
    // 昨天
    if (daysDiff === 1) {
        return '昨天';
    }
    
    // 小于30天：显示天数
    if (diffDays < 30) {
        return diffDays + '天前';
    }
    
    // 计算月份差
    const monthsDiff = (now.getFullYear() - targetTime.getFullYear()) * 12 + (now.getMonth() - targetTime.getMonth());
    
    // 小于12个月：显示月数
    if (monthsDiff < 12) {
        return monthsDiff + '个月前';
    }
    
    // 大于等于12个月：显示年数
    const yearsDiff = Math.floor(monthsDiff / 12);
    return yearsDiff + '年前';
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

// ******************* 全局定时刷新管理器 ***************************
/**
 * 全局定时刷新管理器
 * 全局只启动一个定时任务，间隔固定5秒执行一次
 * 每个页面可以注册自己的刷新函数，只刷新当前显示页面的内容
 */
const PageRefreshManager = (function() {
    let refreshTimer = null;
    let currentRefreshFn = null;
    const REFRESH_INTERVAL = 5000; // 5秒

    // 启动定时器
    function startTimer() {
        if (refreshTimer === null) {
            refreshTimer = setInterval(function() {
                if (currentRefreshFn && typeof currentRefreshFn === 'function') {
                    try {
                        currentRefreshFn();
                    } catch (e) {
                        console.error('页面刷新函数执行错误:', e);
                    }
                }
            }, REFRESH_INTERVAL);
        }
    }

    // 停止定时器
    function stopTimer() {
        if (refreshTimer !== null) {
            clearInterval(refreshTimer);
            refreshTimer = null;
        }
    }

    return {
        /**
         * 注册当前页面的刷新函数
         * @param {Function} refreshFn - 刷新函数
         */
        register: function(refreshFn) {
            currentRefreshFn = refreshFn;
            startTimer();
        },
        
        /**
         * 取消注册当前页面的刷新函数
         */
        unregister: function() {
            currentRefreshFn = null;
        },
        
        /**
         * 停止定时器（通常不需要手动调用）
         */
        stop: function() {
            stopTimer();
            currentRefreshFn = null;
        }
    };
})();

// 全局加载页面
function doLoader(url) {
    // 使用统一的内容区域
    if ($mainContent.length) {
        // 清除之前页面的刷新函数注册
        PageRefreshManager.unregister();
        
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
    return selector.dbSelect({
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
    doGetter("/user/getUserInfo.json", {}, function (response) {
        if (response.success) {
            $("#currentUser").text(response.data.nickname);
        }
    });
}

// 刷新授权信息
function refreshLicense() {
    // 刷新授权信息
    doGetter("/license/query.json", {}, function (response) {
        if (response.success === true) {
            // 社区版
            if (isBlank(response.data.key)) {
                return;
            }
            $("#licenseInfo").show();
            // 专业版
            const licenseInfo = response.data;
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

// ******************* 驱动表格展示内容 ***************************
function showMappingError(metaId){
    doLoader('/monitor?dataStatus=0&id='+metaId);
    // 激活监控菜单
    $('.sidebar-item').removeClass('active');
    $('.sidebar-item[url="/monitor"]').addClass('active');
}

function getStateConfig(mappingId) {
    return {
        0: { // 未运行
            icon: 'fa-play',
            title: '启动',
            onclick: `changeMappingState('${mappingId}', '/mapping/start', '启动')`,
            disabled: false,
            class: 'badge-info',
            text: '未运行'
        },
        1: { // 运行中
            icon: 'fa-pause',
            title: '停止',
            onclick: `changeMappingState('${mappingId}', '/mapping/stop', '停止')`,
            disabled: false,
            class: 'badge-success',
            text: '运行中'
        },
        2: { // 停止中
            icon: 'fa-spinner fa-spin',
            title: '停止中',
            text: ' 停止中',
            disabled: true,
            class: 'badge-warning'
        }
    };
}

// 格式化百分比（保留2位小数）
function formatPercent(value, maxFractionDigits) {
    if (value == null || isNaN(value)) return '0%';
    const percent = value * 100;
    return percent.toFixed(maxFractionDigits || 2) + '%';
}

// 根据同步结果生成内容
function renderSyncResult(mapping) {
    const meta = mapping.meta;
    if (!meta) return '';
    const content = [];
    // 全量模式显示总数
    if (mapping.model === 'full') {
        const total = meta.total || 0;
        content.push(`总数: ${total}`);
        if (meta.counting) {
            content.push(`(正在统计中)`);
        }
        const success = meta.success || 0;
        const fail = meta.fail || 0;
        // 执行中，显示进度
        if (total > 0 && (success + fail) > 0) {
            const progress = (success + fail) / total;
            content.push(`<br />进度: ${formatPercent(progress, 2)}`);
            const beginTime = meta.beginTime;
            const endTime = meta.endTime;
            if (beginTime && endTime) {
                const seconds = Math.floor((endTime - beginTime) / 1000);
                if (seconds < 60) {
                    content.push(`<br />耗时: ${seconds}秒`);
                } else {
                    const minutes = Math.floor(seconds / 60);
                    const remainingSeconds = seconds % 60;
                    content.push(`<br />耗时: ${minutes}分${remainingSeconds}秒`);
                }
            }
        }
        // 成功数量
        content.push(`<br />成功: ${meta.success}`);
    } else {
        // 成功数量
        content.push(`成功: ${meta.success}`);
    }
    // 失败数量
    if (meta.fail > 0) {
        content.push(`失败: <span class="text-error">${meta.fail}</span> <span class="hover-underline cursor-pointer text-error" title='查看失败日志' onclick="showMappingError('${meta.id}')">查看日志</span>`);
    }
    return content.join(' ');
}

// 根据任务类型生成内容
function renderModelText(model) {
    const modelState = {
        'full': { // 全量
            class: 'badge-primary',
            text: '全量同步',
        },
        'increment': { // 增量
            class: 'badge-info',
            text: '增量同步',
        }
    };
    const config = modelState[model];
    return `<span class="badge ${config.class}">${config.text}</span>`;
}

// 根据状态生成内容
function renderStateText(state) {
    const stateConfig = getStateConfig();
    const config = stateConfig[state] || stateConfig[0];
    return `<span class="badge ${config.class}">${config.text}</span>`;
}

// 根据状态生成操作按钮
function renderStateButton(state, mappingId) {
    const stateConfig = getStateConfig(mappingId);
    const config = stateConfig[state] || stateConfig[0];
    const disabledAttr = config.disabled ? ' disabled' : '';
    const onclickAttr = config.onclick ? ` onclick="${config.onclick}"` : '';
    return `
        <button class="table-action-btn play" data-id="${mappingId}" title="${config.title}"${onclickAttr}${disabledAttr}>
            <i class="fa ${config.icon}"></i>
        </button>
    `;
}

$(function () {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/index');
    };
    window.validateForm = validateForm;

    refreshLoginUser();
    refreshLicense();
    // 初始化版权信息
    doGetter("/index/version.json", {}, function (response) {
        if (response.success === true) {
            // 获取底部版权信息
            $("#copyRight").html(response.data.appCopyRight);
            settings.watermark_txt = response.data.watermark;
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