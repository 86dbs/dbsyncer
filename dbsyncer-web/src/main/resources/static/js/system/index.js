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
    $('.dbsyncer_switch').each(function () {
        var $input = $(this);
        $input.attr('role', 'switch');
        $input.attr('aria-checked', $input.is(':checked'));
        $input.on('change', function () {
            $input.attr('aria-checked', this.checked);
        });
    });
    //保存
    $("#updateSystemSubBtn").click(function () {
        const $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            const data = $form.serializeJson();
            submit(data);
        }
    });
})