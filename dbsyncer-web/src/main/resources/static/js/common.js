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

// 跳转菜单
function activeMenu($url){
    var $menu = $('#menu > li');
    $menu.removeClass('active');
    $menu.find("a[url='" + $url + "']").parent().addClass('active');
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

// ******************* 常量配置 ***************************
// 连接器类型
var ConnectorConstant = {
    "Mysql" : "/connector/page/addMysql",
    "Oracle" : "/connector/page/addOracle",
    "SqlServer" : "/connector/page/addSqlServer",
    "DqlMysql" : "/connector/page/addDqlMysql",
    "DqlOracle" : "/connector/page/addDqlOracle",
    "Redis" : "/connector/page/addRedis"
}