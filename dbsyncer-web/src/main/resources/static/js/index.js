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
        // 加载页面
        doLoader($(this).attr("url"), route);
        // 加载页面
        const contents = document.querySelectorAll('.contentDiv');
        contents.forEach(function (content) {
            content.classList.add('hidden');
        });
        if (route === 1) {
            if (timer != null) {
                clearInterval(timer);
                timer = null;
            }
        } else if (route === 2) {
            if (timer2 != null) {
                clearInterval(timer2);
                timer2 = null;
            }
        }
        const contentToShow = $('#initContainer' + route);
        if (contentToShow) {
            contentToShow.removeClass('hidden');
        }

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