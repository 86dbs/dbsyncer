// ******************* 初始化 *****************************
// 默认绑定菜单事件
// 获取项目地址
const $location = (window.location + '').split('/');
const $path = document.location.pathname;
const $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/") + 1);

// 全局加载页面
function doLoader(url) {
    // 使用统一的内容区域
    const contentToShow = $('#mainContent');
    if (contentToShow.length) {
        // 显示加载状态
        $.loadingT(true);

        // 加载页面内容
        contentToShow.load($basePath + url, function (response, status, xhr) {
            $.loadingT(false);
            if (status !== 'success') {
                alert('页面加载失败，请稍后重试');
            }
        });
    }
}

function doPoster(url, params, action) {
    $.loadingT(true);
    $.post($basePath + url, params, function (data) {
        $.loadingT(false);
        action(data);
    }).error(function (xhr, status, info) {
        $.loadingT(false);
        alert("访问异常，请刷新或重试.");
    });
}

$(function () {
    // // 刷新登录用户
    // refreshLoginUser();
    // // 刷新授权信息
    // refreshLicenseInfo();

    // // 初始化版权信息
    // doGetter("/index/version.json", {}, function (data) {
    //     if (data.success == true) {
    //         // 获取底部版权信息
    //         $("#appCopyRight").html(data.resultValue.appCopyRight);
    //         settings.watermark_txt = data.resultValue.watermark;
    //         watermark();
    //     }
    // });

    // 修改登录用户
    $("#edit_personal").click(function () {
        doLoader("/user/page/edit?username=" + $(this).attr("username"));
    });
    //
    // 注销
    $("#nav_logout").click(function () {
        doPoster("/logout", null, function (data) {
            location.href = $basePath;
        });
    });

    // 新导航链接点击事件
    $('.sidebar-item[url]').on('click', function(e) {
        e.preventDefault();
        const url = $(this).attr('url');
        // 更新活动状态
        $('.sidebar-item').removeClass('active');
        $(this).addClass('active');
        // 加载页面
        doLoader(url);
    });
});