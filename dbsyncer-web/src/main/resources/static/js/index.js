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
        updateHash("/user/page/edit?username=" + $(this).attr("username"));
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
                        location.replace($basePath);
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
        // 使用updateHash更新URL哈希而不是直接调用doLoader
        updateHash($(this).attr("url"), route);
        
        // 设置活动菜单项
        const $menu = $("#menu > li");
        $menu.removeClass('active');
        
        // 如果是下拉菜单中的菜单项，给父菜单项添加active类
        const $parentLi = $(this).parent();
        if ($parentLi.parents('.dropdown').length > 0) {
            $parentLi.parents('.dropdown').addClass('active');
        } else {
            $parentLi.addClass('active');
        }
    });

    // 初始化哈希路由
    initHashRouter();
    
    // 如果没有哈希，则显示主页
    if (!window.location.hash || window.location.hash === '#') {
        backIndexPage();
    }
});

// 处理哈希变化事件
function handleHashChange() {
    // 获取当前哈希值
    const hashValue = window.location.hash;
    
    // 如果哈希为空，则返回主页
    if (!hashValue || hashValue === '#') {
        backIndexPage();
        return;
    }
    
    // 解析哈希值，提取URL和路由参数
    const hashPath = hashValue.substring(1); // 去掉#号
    
    // 解析路由参数
    let url = hashPath;
    let route = 0;
    
    // 兼容的URL参数解析方法
    const hashParts = hashPath.split('?');
    const basePath = hashParts[0];
    let paramsString = hashParts[1] || '';
    
    // 解析route参数
    const routeMatch = paramsString.match(/[?&]route=(\d+)/);
    if (routeMatch) {
        route = parseInt(routeMatch[1]);
        // 移除route参数，保留其他所有参数
        paramsString = paramsString.replace(/[?&]route=\d+/, '');
    }
    
    // 重新构建URL，确保参数分隔符正确
    if (paramsString) {
        // 如果paramsString非空，确保以?开头
        if (paramsString.charAt(0) !== '?') {
            paramsString = '?' + paramsString;
        }
    }
    
    // 重新构建URL
    url = basePath + paramsString;
    
    // 根据URL路径设置导航栏活动状态
    const $menu = $("#menu > li");
    $menu.removeClass('active');
    
    // 查找匹配的菜单项并设置为活动状态
    const baseUrl = url.split('?')[0];
    const matchingMenuItem = $("#menu li a[url^='" + baseUrl + "']").parent();
    
    if (matchingMenuItem.length > 0) {
        // 如果是下拉菜单中的菜单项，也需要给父菜单项添加active类
        if (matchingMenuItem.parents('.dropdown').length > 0) {
            matchingMenuItem.parents('.dropdown').addClass('active');
        } else {
            matchingMenuItem.addClass('active');
        }
    }
    
    // 使用不更新哈希的加载函数，避免循环触发hashchange事件
    doLoaderWithoutHashUpdate(url, route);
}

// 初始化哈希路由
function initHashRouter() {
    // 添加哈希变化事件监听器
    window.addEventListener('hashchange', handleHashChange, false);
    
    // 初始化时检查当前哈希
    handleHashChange();
}