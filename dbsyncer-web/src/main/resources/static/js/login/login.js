// URL处理：清除哈希参数和多余的历史记录
(function() {
    // 1. 检查并清除URL中的哈希参数
    if (window.location.hash) {
        // 构建干净的登录页面URL
        var cleanUrl = window.location.protocol + '//' + window.location.host + window.location.pathname;
        // 重定向到干净的URL，替换当前历史记录
        window.location.replace(cleanUrl);
        return;
    }
    
    // 2. 清除多余的浏览器历史记录
    // 当登录页面是浏览器历史记录的最新条目时，清除所有之前的历史记录
    // 这样用户只能回退到浏览器的初始页面
    if (window.history && window.history.length > 1) {
        // 使用replaceState修改当前历史记录条目
        window.history.replaceState(null, null, window.location.href);
        // 添加一个新的历史记录条目，与当前URL相同
        window.history.pushState(null, null, window.location.href);
        // 监听popstate事件，当用户点击回退按钮时
        window.addEventListener('popstate', function() {
            // 检查是否回到了第一个历史记录条目
            if (window.history.state === null) {
                // 再次添加一个历史记录条目
                window.history.pushState(null, null, window.location.href);
            }
        });
    }
})();

// 获取项目地址
var $location = (window.location + '').split('/');
var $path = document.location.pathname;
var $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/")+1);

$(document).ready(function () {
    // token过期，跳转默认页面
    var $navbar = $("div[class='navbar-header']");
    if($navbar.length && $navbar.length>0){
        location.replace($basePath);
        return;
    }
    // 显示登录表单
    var $loginFormHtml = "<form id=\"loginForm\" name=\"loginForm\" method=\"post\">" +
        "<dl class=\"admin_login\">" +
        "<dt><strong id=\"appName\" /></dt>" +
        "<div id=\"loginTip\" class=\"loginTextTip\"></div>" +
        "<dd><input type=\"text\" name=\"username\" placeholder=\"请输入帐号\" class=\"loginInput required\" /></dd>" +
        "<dd><input type=\"password\" name=\"password\" placeholder=\"请输入密码\" class=\"loginInput required\" /></dd>" +
        "<dd><input type=\"button\" value=\"登录\" class=\"submit_btn\" id=\"loginSubmitBtn\" /></dd>" +
        "</dl>" +
        "</form>" +
        "<div class=\"footerContainer\">" +
        "<p id=\"appCopyRight\" style=\"text-align:center;\"></p>" +
        "<p>&nbsp;</p>" +
        "</div>";
    $("body").html($loginFormHtml);
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"]').PlaceHolder({zIndex: '0', top: '12px', left: '14px', fontSize: '15px', color: '#999'});
    // 提交表单
    var $loginSubBtn = $("#loginSubmitBtn");
    $loginSubBtn.click(function () {
        login($("#loginForm"));
    });
    $("input").keydown(function (e) {
        if (e.which == 13) {
            $loginSubBtn.trigger("click");
        }
    });
    // 初始化加载版权信息
    initLoginCopyrightInfo();
});

//初始化加载版权信息
function initLoginCopyrightInfo() {
    $.get($basePath + "/index/version.json", {}, function (data) {
        if (data.success == true) {
            // 获取头部版权信息
            $("#appName").html(data.resultValue.appName);
            // 获取底部版权信息
            $("#appCopyRight").html(data.resultValue.appCopyRight);
        }
    });
}

function showResponse($form, data) {
    $form.find("input").removeAttr('disabled');
    if (data.success == true) {
        // 清除之前的页面记录，确保登录后跳转到首页
        sessionStorage.removeItem('previousPage');
        location.replace($basePath);
        return;
    }
    // 请求失败
    $form.find("#loginTip").removeClass("loginTextTip").addClass("loginTextTipError").html(data.resultValue);
    //用户名密码错误清空输入框
    $form.find('input:eq(0)').val("");
    $form.find('input:eq(1)').val("");
    $form.find('input:eq(0)').focus();
}

function login($form) {
    var username = $form.find('input[name="username"]').val();
    var password = $form.find('input[name="password"]').val();
    if (username != "" && password != "") {
        $form.find("#loginTip").removeClass("loginTextTipError").addClass("loginTextTip").html("登录中...");
        $form.find("input").attr("disabled", "disabled");
        // 点击确定确认登录请求后台
        $.post($basePath + "/login", {"username": username, "password": password}, function (data) {
            showResponse($form, data);
        }).error(function (xhr, status, info) {
            var data = {success: false, resultValue: "登录异常，请刷新或重试."};
            showResponse($form, data);
        }).complete(function (xhr, status) {
            showResponse($form, xhr.responseJSON);
        });
    } else {
        //增加非空提示
        $form.find("#loginTip").html('帐号或密码不能为空');
    }

    // 对登录表单进行非空校验
    $form.find("input.required").each(function () {
        if ($(this).val() != "") {
            $(this).removeClass("loginInputError").addClass("loginInput");
            return;
        }
        $(this).removeClass("loginInput").addClass("loginInputError");
    });
    // 触发 keyup 事件
    $form.find("input.required").keyup(function () {
        $(this).removeClass("loginInputError").addClass("loginInput");
        if ($(this).val() == "") {
            $(this).removeClass("loginInput").addClass("loginInputError");
        }
    });
}