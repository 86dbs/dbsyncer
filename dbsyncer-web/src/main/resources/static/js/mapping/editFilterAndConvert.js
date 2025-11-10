// 绑定表格点击删除事件
function bindConfigListClick($del, $callback){
    $del.unbind("click");
    $del.bind('click', function(){
        // 阻止tr触发click事件
        event.cancelBubble=true;
        $(this).parent().parent().remove();
        $callback();
    });
    $callback();
}

// 初始化组合条件事件
function initConditionOperation() {
    // 绑定插件下拉选择事件
    // $conditionOperation.on('changed.bs.select', function (e) {
    //     const $isSql = "sql" === $(this).selectpicker('val');
    //     $("#conditionList").find("tr").each(function (k, v) {
    //         const $opr = $(this).find("td:eq(0)").text();
    //         if ($isSql) {
    //             $(this).remove();
    //         } else if (!$isSql && $opr == 'sql') {
    //             $(this).remove();
    //         }
    //     });
    //     initFilterParams();
    // });
}

// 初始化过滤条件点击事件
function initFilter(){
    bindConfigListClick($(".conditionDelete"), function(){ initFilterParams(); });
}

// 初始化转换配置点击事件
function initConvert(){
    bindConfigListClick($(".convertDelete"), function(){ initConvertParams(); });
}

// 初始化映射关系参数
function initFilterParams(){
    // 生成JSON参数
    var row = [];
    var $conditionList = $("#conditionList");
    $conditionList.find("tr").each(function(k,v){
        var opt = $(this).find("td:eq(0)").text();
        var sf = $(this).find("td:eq(1)").text();
        var filter = $(this).find("td:eq(2)").text();
        var arg = $(this).find("td:eq(3)").text();
        row.push({
           "name": sf,
           "operation": opt,
           "filter": filter,
           "value": arg
         });
    });
    var $conditionTable = $("#conditionTable");
    if (0 >= row.length) {
        $conditionTable.addClass("hidden");
    } else {
        $conditionTable.removeClass("hidden");
    }
    $("#filter").val(JSON.stringify(row));
}

// 初始化映射关系参数
function initConvertParams(){
    // 生成JSON参数
    var row = [];
    var $convertList = $("#convertList");
    $convertList.find("tr").each(function(k,v){
        var convert = $(this).find("td:eq(0)");
        var convertCode = convert.attr("value");
        var convertName = convert.text().replace(/\n/g,'').trim();
        var tf = $(this).find("td:eq(1)").text();
        var args = $(this).find("td:eq(2)").text();
        row.push({
           "name": tf,
           "convertName": convertName,
           "convertCode": convertCode,
           "args": args
         });
    });
    var $convertTable = $("#convertTable");
    if (0 >= row.length) {
        $convertTable.addClass("hidden");
    } else {
        $convertTable.removeClass("hidden");
    }
    $("#convert").val(JSON.stringify(row));
}

// 绑定新增条件点击事件
function bindConditionAddClick() {
    var $conditionAdd = $("#conditionAdd");
    $conditionAdd.unbind("click");
    $conditionAdd.bind('click', function () {
        // 使用原生 val() 方法替代 selectpicker
        var conditionOperation = $("#conditionOperation").val();
        var conditionSourceField = $("#conditionSourceField").val();
        var conditionFilter = $("#conditionFilter").val();
        var conditionArg = $("#conditionArg").val();
        var $conditionList = $("#conditionList");
        // 自定义SQL
        if (conditionOperation == 'sql') {
            if (isBlank(conditionArg)) {
                bootGrowl("参数不能空.", "danger");
                return;
            }
            $conditionList.html('');
            conditionSourceField = '';
            conditionFilter = '';
        }
        // 非空检查
        else if(isBlank(conditionSourceField)){
            bootGrowl("数据源表字段不能空.", "danger");
            return;
        }

        // 检查重复字段
        var repeated = false;
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
        initFilter();
    });
}

// 绑定添加定时系统参数
function bindConditionQuartzFilterAddClick(){
    const $conditionQuartzFilter = $(".conditionQuartzFilter");
    $conditionQuartzFilter.unbind("click");
    $conditionQuartzFilter.bind('click', function () {
        const $tmpVal = $("#conditionArg");
        $tmpVal.val($tmpVal.val() + $(this).text());
    });
}

// 绑定新增转换点击事件
function bindConvertAddClick() {
    var $convertAdd = $("#convertAdd");
    $convertAdd.unbind("click");
    $convertAdd.bind('click', function () {
        var $convertOperator = $("#convertOperator");
        // 使用原生 val() 方法替代 selectpicker
        var convertOperatorVal = $convertOperator.val();
        var convertOperatorText = $convertOperator.find("option:selected")[0].text;
        var convertTargetField = $("#convertTargetField").val();
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
        initConvert();
    });
}

$(function() {
    initConditionOperation();
    // 过滤条件
    initFilter();
    bindConditionAddClick();
    bindConditionQuartzFilterAddClick();
    // 转换配置
    initConvert();
    bindConvertAddClick();
});