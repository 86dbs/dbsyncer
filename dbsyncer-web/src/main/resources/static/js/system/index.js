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

// 绑定开关切换事件
function bindToggleSwitch($switch, $toggle) {
    let $s = $switch.bootstrapSwitch({
        onText: "Yes",
        offText: "No",
        onColor: "success",
        offColor: "info",
        size: "normal",
        onSwitchChange: function (event, state) {
            if (state) {
                $toggle.removeClass("hidden");
            } else {
                $toggle.addClass("hidden");
            }
        }
    });
    if ($s.bootstrapSwitch('state')) {
        $toggle.removeClass("hidden");
    }
}

$(function () {
    initSwitch();
    bindToggleSwitch($("#enableStorageWriteFail"), $("#maxStorageErrorLength"));
    bindToggleSwitch($("#enableWatermark"), $("#watermark"));
    //保存
    $("#updateSystemSubBtn").click(function () {
        const $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            const data = $form.serializeJson();
            submit(data);
        }
    });
})