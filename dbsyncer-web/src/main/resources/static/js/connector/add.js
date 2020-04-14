function submit(data) {
    doPoster("/connector/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增连接器成功!", "success");
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

//切换连接器
function changeConnectorType($this) {
    //连接器类型
    var connType = $this.val();
    //获取连接器配置元素
    var connectorConfig = $this.parent().parent().parent().find(".connectorConfig");
    //清空配置
    connectorConfig.html("");

    //从公共js/common.js配置文件中读取常量
    for (var key in ConnectorConstant) {
        if (connType == key) {
            //加载页面
            connectorConfig.load(ConnectorConstant[key]);
            break;
        }
    }
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select2插件
    var $connectorTypeSelect = $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });
    // 默认渲染连接器页面
    changeConnectorType($connectorTypeSelect);

    //连接器类型切换事件
    $("select[name='connectorType']").change(function () {
        changeConnectorType($(this));
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