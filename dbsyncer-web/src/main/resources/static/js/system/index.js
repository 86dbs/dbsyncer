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
    if (window.DBSyncerTheme) {
        DBSyncerTheme.enhanceSelects(document);
    }
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
        if (window.DBSyncerTheme && DBSyncerTheme.validateForm($form)) {
            const data = $form.serializeJson();
            console.log(data);
            bootGrowl("修改成功!", "success");
            bootGrowl("修改失败!", "danger");
            // submit(data);
        }
    });
})