// 初始化select2插件
function bindSelectEvent($selector){
    $selector.find(".select-control").select2({
        width : "100%",
        theme : "classic"
    });
}

// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    var $del = $(".fieldMappingDelete");
    $del.unbind("click");
    $del.bind('click', function(){
        // 阻止tr触发click事件
        event.cancelBubble=true;
        $(this).parent().parent().remove();
    });
}
// 绑定添加字段映射点击事件
function bindFieldMappingAddClick(){
    var $btn = $("#fieldMappingAdd");
    $btn.bind('click', function(){
        var sField = $("#sourceFieldMapping").select2("val");
        var tField = $("#targetFieldMapping").select2("val");
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
        var trHtml = "<tr title='双击设为主键'><td>" + sField + "</td><td>" + tField + "</td><td><a class='fa fa-remove fa-2x fieldMappingDelete dbsyncer_pointer' title='删除' ></a></td></tr>";
        $fieldMappingList.append(trHtml);
        bindFieldMappingListClick();
    });
}

$(function() {

    // 绑定表字段关系点击事件
    bindFieldMappingListClick();
    bindFieldMappingAddClick();

    // 初始化select2插件
    bindSelectEvent($("#mappingBaseConfig"));
    bindSelectEvent($("#mappingSuperConfig"));

    // 返回按钮，跳转至上个页面
    $("#mappingTableGroupBackBtn").bind('click', function(){
        var $mappingId = $(this).attr("mappingId");
        $initContainer.load('/mapping/page/editMapping?id=' + $mappingId);
    });
});