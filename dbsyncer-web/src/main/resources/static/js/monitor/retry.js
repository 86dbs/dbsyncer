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

// 修改增量数据
function bindRetryDataModifyClick() {
    $(".retryDataModify").click(function () {
        let $key = $(this).attr("type");
        let $tr = $(this).parent().parent();
        let $value = $tr.find("td:eq(3)");
        let tmp = $value.text();
        $value.text("");
        $value.append("<input type='text'/>");
        let $input = $value.find("input");
        $input.focus().val(tmp);
        $input.blur(function () {
            $value.text($(this).val());
            if (tmp != $(this).val()) {
                createRetryDataParams($key, $(this).val());
                $tr.addClass("success");
                $tr.attr("title", "已修改");
            }
            $input.unbind();
        });
    })
}

// 生成修改参数
function createRetryDataParams($key, $value) {
    let $params = $("#retryDataParams");
    let val = $params.val();
    let jsonObj = {};
    if(!isBlank(val)){
        jsonObj = $.parseJSON(val);
    }

    jsonObj[$key] = $value;
    $params.val(JSON.stringify(jsonObj));
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

    bindRetryDataModifyClick();
})