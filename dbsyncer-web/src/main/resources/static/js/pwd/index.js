function submit(data) {
    doPoster('/pwd/edit', data, function (data) {
        if (data.success == true) {
            doPoster("/logout", null, function (data) {
                location.href = $basePath;
            });
        } else {
            bootGrowl(data.resultValue, "danger");
            $initContainer.load("/pwd");
        }
    });
}

$(function () {
    //保存
    $("#updatePwdSubBtn").click(function () {
        var $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

})