// 获取项目地址
var $location = (window.location + '').split('/');
var $path = document.location.pathname;
var $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/")+1);
// 全局内容区域
var $initContainer = $(".contentDiv");
    $initContainer.css("min-height", $(window).height() - 125);

// 监控定时器
var timer;

// ******************* 插件封装 ***************************
// 全局提示框
function bootGrowl(data, type) {
    $.bootstrapGrowl(data, { // data为提示信息
        type: type == undefined ? 'success' : type,// type指提示类型
        delay: 3000,// 提示框显示时间
        allow_dismiss: true // 显示取消提示框
    });
}

// 刷新登录用户信息
function refreshLoginUser() {
    // 获取登录用户信息
    doGetter("/user/getUserInfo.json", {}, function (data) {
        if (data.success == true) {
            $("#currentUser").html(data.resultValue.nickname + " (" + data.resultValue.roleName + ")");
            $("#edit_personal").attr("username", data.resultValue.username);
        }
    });
}

// 跳转主页
function backIndexPage(projectGroupId) {
    // 加载页面
    projectGroupId = (typeof projectGroupId === 'string') ? projectGroupId : '';
    doLoader("/index?projectGroupId="+ projectGroupId +"&refresh=" + new Date().getTime(),1);
}

// 美化SQL
function beautifySql(){
    var $sql = $(".sql");
    var $tmp = $sql.attr('tmp');
    if(null == $tmp){
        $sql.attr('tmp', $sql.val());
        $sql.val(sqlFormatter.format($sql.val()));
        return;
    }
    $sql.val($tmp);
    $sql.removeAttr('tmp');
}

// 初始化select组件，默认选中
function initSelectIndex($select, $selectedIndex){
    let select = initSelect($select);

    if($selectedIndex < 0){
        return select;
    }

    $.each($select, function () {
        var v = $(this).selectpicker('val');
        if (undefined == v || '' == v) {
            var $option = $(this).find("option")[$selectedIndex];
            if(undefined != $option){
                $(this).selectpicker('val', $option.value);
            }
        }
    });
    return select;
}
function initSelect($select){
    return $select.selectpicker({
        "title":"请选择",
        "actionsBox":true,
        "liveSearch":true,
        "selectAllText":"全选",
        "deselectAllText":"取消全选",
        "noneResultsText":"没有找到 {0}",
        "selectedTextFormat":"count > 10"
    });
}
// 绑定多值输入框事件
function initMultipleInputTags() {
    $("input[data-role=tagsinput]").tagsinput({
        maxChars: 32,
        maxTags: 5,
        tagClass: 'label label-success',
        trimValue: true
    });
}

// 初始化开关
function initSwitch() {
    $('.dbsyncer_switch').bootstrapSwitch({
        onText: "Yes",
        offText: "No",
        onColor: "success",
        offColor: "info",
        size: "normal"
    });
}

// ******************* 水印 ***************************
window.onresize = function() {
    watermark();
}
window.onscroll = function() {
    watermark();
}
//水印样式默认设置
const settings={
    watermark_txt:"",
    watermark_x:50,//水印起始位置x轴坐标
    watermark_y:55,//水印起始位置Y轴坐标
    watermark_rows:2000,//水印行数
    watermark_cols:2000,//水印列数
    watermark_x_space:70,//水印x轴间隔
    watermark_y_space:30,//水印y轴间隔
    watermark_color:'#aaaaaa',//水印字体颜色
    watermark_alpha:0.2,//水印透明度
    watermark_fontsize:'15px',//水印字体大小
    watermark_font:'微软雅黑',//水印字体
    watermark_width:210,//水印宽度
    watermark_height:80,//水印长度
    watermark_angle:15//水印倾斜度数
};
let timestampWatermark;
function watermark() {
    const now = Date.now();
    if (timestampWatermark != null && now - timestampWatermark < 200) {
        return;
    }
    if(isBlank(settings.watermark_txt)){
        return;
    }
    timestampWatermark = now;
    $(".dbsyncer_mask").remove();

    const water = document.body;
    //获取页面最大宽度
    const page_width = Math.max(water.scrollWidth,water.clientWidth);
    //获取页面最大高度
    const page_height = Math.max(water.scrollHeight,water.clientHeight);
    //水印数量自适应元素区域尺寸
    settings.watermark_cols=Math.ceil(page_width/(settings.watermark_x_space+settings.watermark_width));
    settings.watermark_rows=Math.ceil(page_height/(settings.watermark_y_space+settings.watermark_height));
    let x;
    let y;
    for (let i = 0; i < settings.watermark_rows; i++) {
        y = settings.watermark_y + (settings.watermark_y_space + settings.watermark_height) * i;
        for (let j = 0; j < settings.watermark_cols; j++) {
            x = settings.watermark_x + (settings.watermark_width + settings.watermark_x_space) * j;
            let mask_div = document.createElement('div');
            mask_div.className = 'dbsyncer_mask';
            mask_div.innerHTML=(settings.watermark_txt);
            mask_div.style.webkitTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.MozTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.msTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.OTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.transform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.position = "absolute";
            mask_div.style.left = x + 'px';
            mask_div.style.top = y + 'px';
            mask_div.style.overflow = "hidden";
            mask_div.style.pointerEvents='none';
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
function doLoader(url,route=0){
    clearInterval(timer);

    // 加载页面
    const contents = document.querySelectorAll('.contentDiv');
    contents.forEach(function(content) {
        content.classList.add('hidden');
    });
    const contentToShow = $('#initContainer' + route);
    if (contentToShow) {
        contentToShow.removeClass('hidden');
    }
    contentToShow.load($basePath + url, function (response, status, xhr) {
        if (status != 'success') {
            bootGrowl(response);
        }
        watermark();
        $.loadingT(false);
    });


}

// 异常请求
function doRequest(action, data){
    $.loadingT(false);
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
    $.loadingT(false);
    bootGrowl("访问异常，请刷新或重试.", "danger");
}

// 全局Ajax post
function doPoster(url, params, action) {
    $.loadingT(true);
    $.post($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

// 全局Ajax get
function doGetter(url, params, action, loading) {
    if(loading == undefined || loading == true){
        $.loadingT(true);
    }
    $.get($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

// 全局Ajax get, 不显示加载动画
function doGetWithoutLoading(url, params, action) {
    doGetter(url, params, action, false);
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