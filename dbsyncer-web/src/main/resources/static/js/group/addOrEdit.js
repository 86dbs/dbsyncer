/**
 *
 * Created by zhichao.qin on 2022/6/15
 */
function submit(data) {
    if (data["id"]) {
        doPoster("/projectGroup/edit", data, function (data) {
            if (data.success == true) {
                bootGrowl("修改分组成功!", "success");
                backIndexPage();
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    } else {
        doPoster("/projectGroup/add", data, function (data) {
            if (data.success == true) {
                bootGrowl("新增分组成功!", "success");
                backIndexPage();
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    }
}

$(function () {
    // 增强所有 select 元素（替代 Bootstrap selectpicker）
    if (typeof enhanceAllSelects === 'function') {
        enhanceAllSelects();
    }

    // 设置初始值（使用原生 jQuery val() 方法替代 selectpicker）
    var $connectorIds = $("#connectorIds");
    var $mappingIds = $("#mappingIds");
    
    if ($connectorIds.length) {
        var connectorValues = splitStrByDelimiter($("#selectedConnectorIds").val(), ",");
        $connectorIds.val(connectorValues);
    }
    
    if ($mappingIds.length) {
        var mappingValues = splitStrByDelimiter($("#selectedMappingIds").val(), ",");
        $mappingIds.val(mappingValues);
    }

    //保存
    $("#projectGroupSubmitBtn").click(function () {
        var $form = $("#projectGroupAddForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            if (data.connectorIds instanceof Array) {
                data.connectorIds = data.connectorIds.join('|');
            }
            if (data.mappingIds instanceof Array) {
                data.mappingIds = data.mappingIds.join('|');
            }
            submit(data);
        }
    });

    //返回
    $("#projectGroupBackBtn").click(function () {
        backIndexPage();
    });
})