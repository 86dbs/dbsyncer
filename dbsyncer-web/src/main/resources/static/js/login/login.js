// 获取项目地址
var $location = (window.location + '').split('/');
var $path = document.location.pathname;
var $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/")+1);

$(document).ready(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"]').PlaceHolder({
        zIndex: '0',
        top: '12px',
        left: '14px',
        fontSize: '15px',
        color: '#999'
    });

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
        // 加载页面
        location.href = $basePath;
    } else {
        // 请求失败
        $form.find("#loginTip").removeClass("loginTextTip").addClass("loginTextTipError").html(data.resultValue);
        //用户名密码错误清空输入框
        $form.find('input:eq(0)').val("");
        $form.find('input:eq(1)').val("");
        $form.find('input:eq(0)').focus();
    }
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