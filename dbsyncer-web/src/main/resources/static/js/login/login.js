// 获取项目地址
var $location = (window.location + '').split('/');
var $basePath = $location[0] + '//' + $location[2];

var $formHtml = "<dl class=\"admin_login\">\n" +
    "\t<dt>\n" +
    "\t\t<strong>DBSyncer</strong>\n" +
    "\t</dt>\n" +
    "\t<div id=\"loginTip\" class=\"loginVerifcateTextError\"></div>\n" +
    "\t<dd class=\"user_icon\">\n" +
    "\t\t<input type=\"text\" name=\"username\" placeholder=\"请输入帐号\" class=\"login_txtbx required\" />\n" +
    "\t</dd>\n" +
    "\t<dd class=\"pwd_icon\">\n" +
    "\t\t<input type=\"password\" name=\"password\" placeholder=\"请输入密码\" class=\"login_txtbx required\" />\n" +
    "\t</dd>\n" +
    "\t<dd>\n" +
    "\t\t<input type=\"button\" value=\"登录\" class=\"submit_btn\" id=\"loginSubmitBtn\" />\n" +
    "\t</dd>\n" +
    "</dl>";

$(document).ready(function () {
    // 会话过期
    var html = $("#logoName").html();
    if (html != undefined && html != null) {
        location.href = $basePath;
        return;
    }

    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"]').PlaceHolder({
        zIndex: '0',
        top: '12px',
        left: '14px',
        fontSize: '15px',
        color: '#999'
    });

    var $form = $("#loginForm");
    $form.html($formHtml);
    // 提交表单
    var $loginSubBtn = $("#loginSubmitBtn");
    $loginSubBtn.click(function () {
        login($form);
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
    $.getJSON("/config/system.json", function (data) {
        // 获取底部版权信息
        $("#loginCopyrightInfo").html(data.footerName);
    });
}

function showResponse($form, data) {
    $form.find("input").removeAttr('disabled');
    if (data.success == true) {
        // 加载页面
        location.href = $basePath;
    } else {
        // 请求失败
        $form.find("#loginTip").removeClass("loginVerifcateTextTip").addClass("loginVerifcateTextError").html(data.resultValue);
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
        $form.find("#loginTip").removeClass("loginVerifcateTextError").addClass("loginVerifcateTextTip").html("登录中...");
        $form.find("input").attr("disabled", "disabled");
        // 点击确定确认登录请求后台
        $.post("/login", {"username": username, "password": password}, function (data) {
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
            $(this).removeClass("loginVerifcateError");
            $(this).addClass("login_txtbx");
            return;
        }
        $(this).removeClass("login_txtbx");
        $(this).addClass("loginVerifcateError");
    });
    // 触发 keyup 事件
    $form.find("input.required").keyup(function () {
        $(this).removeClass("loginVerifcateError");
        $(this).addClass("login_txtbx");
        if ($(this).val() == "") {
            $(this).removeClass("login_txtbx");
            $(this).addClass("loginVerifcateError");
        }
    });
}