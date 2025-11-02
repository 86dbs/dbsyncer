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
    $("#updateSystemBtn").click(function () {
        const $form = $("#config-form-edit");
        if (window.DBSyncerTheme && DBSyncerTheme.validateForm($form)) {
            showLoading();
            doPoster('/system/edit', $form.serializeJson(), function (data) {
                hideLoading();
                if (data.success == true) {
                    bootGrowl("修改成功!", "success");
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
                doLoader("/system");
            });
        }
    });
})