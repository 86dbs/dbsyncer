function submit(data) {
    //保存驱动配置
    doPoster("/tableGroup/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("保存表映射关系成功!", "success");
            backMappingPage($("#tableGroupSubmitBtn"));
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 初始化select2插件
function bindSelectEvent($selector){
    $selector.find(".select-control").select2({
        width : "100%",
        theme : "classic"
    });
}

// 初始化映射关系参数
function initFieldMappingParams(){
    // 生成JSON参数
    var row = [];
    var $fieldMappingList = $("#fieldMappingList");
    $fieldMappingList.find("tr").each(function(k,v){
        var $pk = $(this).find("td:eq(2)").html();
        row.push({
            "source":$(this).find("td:eq(0)").text(),
            "target":$(this).find("td:eq(1)").text(),
            "pk":($pk != "" || $.trim($pk).length > 0)
        });
    });
    $("#fieldMapping").val(JSON.stringify(row));
}
// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    // 行双击事件
    var $tr = $("#fieldMappingList tr");
    $tr.unbind("dblclick");
    $tr.bind('dblclick', function () {
        var $pk = $(this).find("td:eq(2)");
        var $text = $pk.html();
        var isPk = $text == "" || $.trim($text).length == 0;
        $pk.html(isPk ? '<i title="主键" class="fa fa-key fa-fw fa-rotate-90 text-warning"></i>' : '');
        initFieldMappingParams();
    });

    // 删除事件
    var $del = $(".fieldMappingDelete");
    $del.unbind("click");
    $del.bind('click', function(){
        // 阻止tr触发click事件
        event.cancelBubble=true;
        $(this).parent().parent().remove();
        initFieldMappingParams();
    });
}
// 绑定添加字段映射点击事件
function bindFieldMappingAddClick(){
    var $btn = $("#fieldMappingAdd");
    $btn.bind('click', function(){
        var sField = $("#sourceFieldMapping").select2("val");
        var tField = $("#targetFieldMapping").select2("val");
        sField = sField == null ? "" : sField;
        tField = tField == null ? "" : tField;
        // 非空检查
        if(sField == "" && tField == ""){
            bootGrowl("至少有一个表字段.", "danger");
            return;
        }

        // 检查重复字段
        var repeated = false;
        var $fieldMappingList = $("#fieldMappingList");
        $fieldMappingList.find("tr").each(function(k,v){
             var sf = $(this).find("td:eq(0)").text();
             var tf = $(this).find("td:eq(1)").text();
             if(repeated = (sField==sf && tField==tf)){
                bootGrowl("映射关系已存在.", "danger");
                return false;
             }
        });
        if(repeated){ return; }
        var trHtml = "<tr title='双击设置/取消主键'><td>" + sField + "</td><td>" + tField + "</td><td></td><td><a class='fa fa-remove fa-2x fieldMappingDelete dbsyncer_pointer' title='删除' ></a></td></tr>";
        $fieldMappingList.append(trHtml);

        initFieldMappingParams();
        bindFieldMappingListClick();
    });
}
// 绑定下拉自动匹配字段
function bindAutoSelect(){
    var $sourceSelect = $("#sourceFieldMapping");
    var $targetSelect = $("#targetFieldMapping");

    // 绑定数据源下拉切换事件
    $sourceSelect.change(function () {
        var v = $(this).select2("val");
        $targetSelect.val(v).trigger("change");
    });
}
// 返回驱动配置页面
function backMappingPage($this){
    $initContainer.load('/mapping/page/edit?id=' + $this.attr("mappingId"));
}

$(function() {
    // 绑定表字段关系点击事件
    initFieldMappingParams();
    bindFieldMappingListClick();
    bindFieldMappingAddClick();
    // 绑定下拉自动匹配字段
    bindAutoSelect();

    // 初始化select2插件
    bindSelectEvent($("#tableGroupBaseConfig"));
    bindSelectEvent($("#tableGroupSuperConfig"));

    //保存
    $("#tableGroupSubmitBtn").click(function () {
        var $form = $("#tableGroupModifyForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    // 返回按钮，跳转至上个页面
    $("#tableGroupBackBtn").bind('click', function(){
        backMappingPage($(this));
    });
});