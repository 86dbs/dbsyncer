// 绑定表格点击删除事件
function bindConfigListClick($del){
    $del.unbind("click");
    $del.bind('click', function(){
        // 阻止tr触发click事件
        event.cancelBubble=true;
        $(this).parent().parent().remove();
    });
}

// 绑定新增条件点击事件
function bindConditionAddClick() {
    var $conditionAdd = $("#conditionAdd");
        $conditionAdd.unbind("click");
        $conditionAdd.bind('click', function () {
        var conditionOperation = $("#conditionOperation").select2("val");
        var conditionSourceField = $("#conditionSourceField").select2("val");
        var conditionFilter = $("#conditionFilter").select2("val");
        var conditionArg = $("#conditionArg").val();
        // 非空检查
        if(conditionSourceField == null || conditionSourceField == undefined || conditionSourceField == ''){
            bootGrowl("数据源表字段不能空.", "danger");
            return;
        }

        // 检查重复字段
        var repeated = false;
        var $conditionList = $("#conditionList");
        $conditionList.find("tr").each(function(k,v){
             var opr = $(this).find("td:eq(0)").text();
             var sf = $(this).find("td:eq(1)").text();
             var filter = $(this).find("td:eq(2)").text();
             var arg = $(this).find("td:eq(3)").text();
             if(repeated = (opr==conditionOperation && sf==conditionSourceField && filter==conditionFilter && arg==conditionArg)){
                bootGrowl("过滤条件已存在.", "danger");
                // break;
                return false;
             }
        });
        if(repeated){ return; }

        var trHtml = "<tr>";
        trHtml += "<td>" + conditionOperation + "</td>";
        trHtml += "<td>" + conditionSourceField + "</td>";
        trHtml += "<td>" + conditionFilter + "</td>";
        trHtml += "<td>" + conditionArg + "</td>";
        trHtml += "<td><a class='fa fa-remove fa-2x conditionDelete dbsyncer_pointer' title='删除' ></a></td>";
        trHtml += "</tr>";
        $conditionList.append(trHtml);
        // 清空参数
        $("#conditionArg").val("");
        bindConditionListClick($(".conditionDelete"));
    });
}

// 绑定新增转换点击事件
function bindConvertAddClick() {
    var $convertAdd = $("#convertAdd");
        $convertAdd.unbind("click");
        $convertAdd.bind('click', function () {
        var $convertOperator = $("#convertOperator");
        var convertOperatorVal = $convertOperator.select2("val");
        var convertOperatorText = $convertOperator.select2("data")[0].text;
        var convertTargetField = $("#convertTargetField").select2("val");
        var convertArg = $(".convertArg:eq(0)").val();
        var convertArg1 = $(".convertArg:eq(1)").val();
        // 多个参数时，英文符号“,”拼接
        convertArg = convertArg1 !== '' ? convertArg + ','+ convertArg1 : convertArg;
        // 非空检查
        if(convertTargetField == null || convertTargetField == undefined || convertTargetField == ''){
            bootGrowl("目标源表字段不能空.", "danger");
            return;
        }

        // 检查重复字段
        var repeated = false;
        var $convertList = $("#convertList");
        $convertList.find("tr").each(function(k,v){
             var opr = $(this).find("td:eq(0)").text();
             var tf = $(this).find("td:eq(1)").text();
             var arg = $(this).find("td:eq(2)").text();
             if(repeated = (opr==convertOperatorText && tf==convertTargetField && arg==convertArg)){
                bootGrowl("转换配置已存在.", "danger");
                // break;
                return false;
             }
        });
        if(repeated){ return; }

        var trHtml = "<tr>";
        trHtml += "<td value='" + convertOperatorVal + "'>" + convertOperatorText + "</td>";
        trHtml += "<td>" + convertTargetField + "</td>";
        trHtml += "<td>" + convertArg + "</td>";
        trHtml += "<td><a class='fa fa-remove fa-2x convertDelete dbsyncer_pointer' title='删除' ></a></td>";
        trHtml += "</tr>";
        $convertList.append(trHtml);
        // 清空参数
        $(".convertArg").val("");
        bindConfigListClick($(".convertDelete"));
    });
}

$(function() {
    // 过滤条件
    bindConfigListClick($(".conditionDelete"));
    bindConditionAddClick();

    // 转换配置
    bindConfigListClick($(".convertDelete"));
    bindConvertAddClick();
});