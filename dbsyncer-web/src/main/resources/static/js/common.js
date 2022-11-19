// 获取项目地址
var $location = (window.location + '').split('/');
var $path = document.location.pathname;
var $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/")+1);
// 全局内容区域
var $initContainer = $("#initContainer");
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
    doLoader("/index?projectGroupId="+ projectGroupId +"&refresh=" + new Date().getTime());
}

// 美化SQL
function beautifySql(){
    var $sql = $("#sql");
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
    initSelect($select);

    if($selectedIndex < 0){
        return;
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
}
function initSelect($select){
    $select.selectpicker({
        "style":'dbsyncer_btn-info',
        "title":"请选择",
        "actionsBox":true,
        "liveSearch":true,
        "selectAllText":"全选",
        "deselectAllText":"取消全选",
        "noneResultsText":"没有找到 {0}",
        "selectedTextFormat":"count > 10"
    });
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
function doLoader(url){
    clearInterval(timer);
    // 加载页面
    $initContainer.load($basePath + url, function (response, status, xhr) {
        if (status != 'success') {
            bootGrowl(response);
        }
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