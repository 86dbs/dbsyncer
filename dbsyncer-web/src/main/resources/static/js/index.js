// ******************* 初始化 *****************************
// 默认绑定菜单事件
$(function () {
    // 刷新登录用户
    refreshLoginUser();
    // 刷新授权信息
    refreshLicenseInfo();

    // 初始化版权信息
    doGetter("/index/version.json", {}, function (data) {
        if (data.success == true) {
            // 获取底部版权信息
            $("#appCopyRight").html(data.resultValue.appCopyRight);
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
        var route = $(this).data('route');
        var url = $(this).attr("url");
        
        // 清理定时器 - 只有在离开index页面时才清理
        if (url !== '/index' && url !== '/') {
            if (timer != null) {
                clearInterval(timer);
                timer = null;
            }
            if (timer2 != null) {
                clearInterval(timer2);
                timer2 = null;
            }
        }
        
        // 加载页面（doLoader函数现在处理内容区域管理）
        doLoader(url, route);
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