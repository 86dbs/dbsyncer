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

// 绑定水印开关切换事件
function bindWatermarkSwitch() {
    const $watermark = $("#watermark");
    let $switch = $("#enableWatermark").bootstrapSwitch({
        onText: "Yes",
        offText: "No",
        onColor: "success",
        offColor: "info",
        size: "normal",
        onSwitchChange: function (event, state) {
            if (state) {
                $watermark.removeClass("hidden");
            } else {
                $watermark.addClass("hidden");
            }
        }
    });
    if ($switch.bootstrapSwitch('state')) {
        $watermark.removeClass("hidden");
    }
}

$(function () {
    initSwitch();
    bindWatermarkSwitch();
    //保存
    $("#updateSystemSubBtn").click(function () {
        const $form = $("#configEditForm");
        if ($form.formValidate() == true) {
            const data = $form.serializeJson();
            submit(data);
        }
    });
})