function submit(data) {
    doPoster("/connector/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增连接成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

var check = function () {
    var $form = $("#connectorAddForm");
    if ($form.formValidate() == true) {
        var data = $form.serializeJson();
        submit(data);
    }
};

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    $(".select-control").selectpicker({
        "title":"请选择",
        "actionsBox":true,
        "liveSearch":true,
        "selectAllText":"全选",
        "deselectAllText":"取消全选",
        "noneResultsText":"没有找到 {0}",
        "selectedTextFormat":"count > 10"
    });

    // 默认渲染连接页面
    $("#connectorType").on('changed.bs.select',function(e){
        //连接类型
        var connType = $(this).selectpicker('val');
        //获取连接配置元素
        var $connectorConfig = $("#connectorConfig");
        //清空配置
        $connectorConfig.html("");

        //加载页面
        $connectorConfig.load($basePath + "/connector/page/add" + connType);
    });

    //保存
    $("#connectorSubmitBtn").click(function () {
        check();
    });

    //返回
    $("#connectorBackBtn").click(function () {
        // 显示主页
        backIndexPage();
    });
})