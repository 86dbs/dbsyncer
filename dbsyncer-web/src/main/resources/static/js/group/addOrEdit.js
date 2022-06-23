/**
 *
 * Created by zhichao.qin on 2022/6/15
 */
function submit(data) {
    if (data["id"]) {
        doPoster("/projectGroup/edit", data, function (data) {
            if (data.success == true) {
                bootGrowl("修改项目组成功!", "success");
                backIndexPage();
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    } else {
        doPoster("/projectGroup/add", data, function (data) {
            if (data.success == true) {
                bootGrowl("新增项目组成功!", "success");
                backIndexPage();
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    }
}

function initSelectByValueOrDefault($select, $selectedValue) {

    $.each($select, function () {
        var values = splitStrByDelimiter($selectedValue, ",");
        $(this).selectpicker('val', values);
    });
}

$(function () {
    // 初始化select插件
    initSelect($(".select-control-table"));

    initSelectByValueOrDefault($("#connectorIds"), $("#selectedConnectors").val());
    initSelectByValueOrDefault($("#mappingIds"), $("#selectedMappings").val());

    //保存
    $("#groupSubmitBtn").click(function () {
        var $form = $("#groupAddForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            var connectorIds = data['connectorIds'];
            var mappingIds = data['mappingIds'];
            data['connectorIds'] = (connectorIds instanceof Array) ? connectorIds.join(',') : connectorIds;
            data['mappingIds'] = (mappingIds instanceof Array) ? mappingIds.join(',') : mappingIds;
            submit(data);
        }
    });

    //返回
    $("#groupBackBtn").click(function () {
        backIndexPage();
    });
})
