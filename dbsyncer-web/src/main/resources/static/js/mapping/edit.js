function submit(data) {
    //保存驱动配置
    doPoster("/mapping/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("保存驱动成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    //保存
    $("#mappingSubmitBtn").click(function () {
        var $form = $("#mappingModifyForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#mappingBackBtn").click(function () {
        backIndexPage();
    });
})