function submit(data) {
    doPoster("/connector/edit", data, function (response) {
        if (response.success === true) {
            bootGrowl("保存成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(response.resultValue || '保存失败', "danger");
        }
    });
}

$(function () {
    //保存
    $("#connectorSubmitBtn").click(function () {
        const $form = $("#connectorModifyForm");
        if ($form.formValidate() === true) {
            submit($form.serializeJson());
        }
    });

    //返回
    $("#connectorBackBtn").click(function () {
        // 显示主页
        backIndexPage();
    });
})