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

// 绑定连接器类型切换事件
function bindConnectorChangeEvent($select) {
    // 默认渲染连接页面
    $select.on('changed.bs.select',function(e){
        changeConnectorType($(this));
    });

    changeConnectorType($select);
}

function changeConnectorType($select){
    //连接类型
    var connType = $select.selectpicker('val');
    //获取连接配置元素
    var $connectorConfig = $("#connectorConfig");
    //清空配置
    $connectorConfig.html("");

    //加载页面
    $connectorConfig.load($basePath + "/connector/page/add" + connType);
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    var $select = $("#connectorType");
    initSelectIndex($select, 1);
    // 绑定连接器类型切换事件
    bindConnectorChangeEvent($select);

    //保存
    $("#connectorSubmitBtn").click(function () {
        var $form = $("#connectorAddForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#connectorBackBtn").click(function () {
        // 显示主页
        backIndexPage();
    });
})