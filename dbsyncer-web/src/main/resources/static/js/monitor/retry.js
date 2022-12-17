function submit(data) {
    doPoster("/monitor/sync", data, function (data) {
        if (data.success == true) {
            bootGrowl("正在同步中.", "success");
            backMonitorPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

function backMonitorPage() {
    doLoader('/monitor?id=' + $("#metaId").val());
}

$(function () {
    //保存
    $("#retryDataSubmitBtn").click(function () {
        var $form = $("#retryDataForm");
        var data = $form.serializeJson();
        submit(data);
    });

    //返回
    $("#retryDataBackBtn").click(function () {
        backMonitorPage();
    });
})