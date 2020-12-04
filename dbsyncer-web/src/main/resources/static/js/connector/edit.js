function submit(data) {
    doPoster("/connector/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("修改连接器成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select2插件
    var $connectorTypeSelect = $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });
    //保存
    $("#connectorSubmitBtn").click(function () {
        var $form = $("#connectorModifyForm");
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