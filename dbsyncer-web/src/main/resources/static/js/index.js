// 获取项目地址
var $location = (window.location + '').split('/');
var $basePath = $location[0] + '//' + $location[2];

// 全局内容区域
var $initContainer = $("#initContainer");

// ******************* 插件封装 ***************************
// 全局提示框
function bootGrowl(data, type) {
    $.bootstrapGrowl(data, { // data为提示信息
        type: type == undefined ? 'success' : type,// type指提示类型
        delay: 1000,// 提示框显示时间
        allow_dismiss: true // 显示取消提示框
    });
}

// 跳转主页
function backIndexPage() {
    // 加载页面
    $initContainer.load("/index?refresh=" + new Date().getTime());
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

// 全局Ajax post
function doPoster(url, params, action) {
    $.loadingT(true);
    $.post(url, params, function (data) {
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
    }).error(function (xhr, status, info) {
        $.loadingT(false);
        bootGrowl("访问异常，请刷新或重试.", "danger");
    });
}

// ******************* 初始化 *****************************
// 默认绑定菜单事件
$(function () {

    // 初始化版权信息
    $.getJSON("/config/system.json", function (data) {
        // 获取头部版权信息
        $("#logoName").html(data.headName);
        // 获取底部版权信息
        $("#copyrightInfo").html(data.footerName);
    });

    // 注销
    $("#nav_logout").click(function () {
        // 确认框确认是否注销
        BootstrapDialog.show({
            title: "提示",
            type: BootstrapDialog.TYPE_INFO,
            message: "确认注销帐号？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster("/logout", null, function (data) {
                        location.href = $basePath;
                    });
                    dialog.close();
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });

    // 绑定所有的菜单链接点击事件，根据不同的URL加载页面
    $("#menu li a[url]").click(function () {
        // 加载页面
        $initContainer.load($(this).attr("url"));
    });

    // 头部导航栏选中切换事件
    var $menu = $('#menu > li');
    $menu.click(function () {
        $menu.removeClass('active');
        $(this).addClass('active');
    });

    // 显示主页
    backIndexPage();
});