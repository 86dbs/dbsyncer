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
        showEmpty: showEmpty
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
});