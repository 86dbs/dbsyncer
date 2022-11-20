function submit(data) {
    doPoster('/system/edit', data, function (data) {
        if (data.success == true) {
            bootGrowl("修改成功!", "success");
        } else {
            bootGrowl(data.resultValue, "danger");
        }
        doLoader("/system");
    });
}

$(function () {
    //保存
    $("#updateSystemSubBtn").click(function () {
        var $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

})