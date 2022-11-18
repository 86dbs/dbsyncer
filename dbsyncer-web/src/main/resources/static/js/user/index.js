function submit(data) {
    doPoster('/user/add', data, function (data) {
        if (data.success == true) {
            doPoster("/logout", null, function (data) {
                location.href = $basePath;
            });
        } else {
            bootGrowl(data.resultValue, "danger");
            doLoader("/user");
        }
    });
}

$(function () {
    //保存
    $("#updateUserBtn").click(function () {
        var $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

})