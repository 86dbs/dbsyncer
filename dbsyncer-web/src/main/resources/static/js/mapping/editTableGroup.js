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

// 获取选择的CheckBox[value]
function getCheckedBoxSize($checkbox){
    var checked = [];
    $checkbox.each(function(){
        if($(this).prop('checked')){
            checked.push($(this).attr("id"));
        }
    });
    return checked;
}
// 绑定删除表字段复选框事件
function bindFieldMappingCheckBoxClick(){
    var $checkboxAll = $('.fieldMappingDeleteCheckboxAll');
    var $checkbox = $('.fieldMappingDeleteCheckbox');
    var $delBtn = $("#fieldMappingDelBtn");
    $checkboxAll.iCheck({
        checkboxClass: 'icheckbox_square-red',
        labelHover: false,
        cursor: true
    }).on('ifChecked', function (event) {
        $checkbox.iCheck('check');
    }).on('ifUnchecked', function (event) {
        $checkbox.iCheck('uncheck');
    }).on('ifChanged', function (event) {
        $delBtn.prop('disabled', getCheckedBoxSize($('.fieldMappingDeleteCheckbox')).length < 1);
    });

    // 初始化icheck插件
    $checkbox.iCheck({
        checkboxClass: 'icheckbox_square-red',
        cursor: true
    }).on('ifChanged', function (event) {
        // 阻止tr触发click事件
        event.stopPropagation();
        event.cancelBubble=true;
        $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
    });
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
}
// 绑定添加字段映射点击事件
function bindFieldMappingAddClick(){
    var $btn = $("#fieldMappingAddBtn");
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
        var $tr = $fieldMappingList.find("tr");
        $tr.each(function(k,v){
             var sf = $(this).find("td:eq(0)").text();
             var tf = $(this).find("td:eq(1)").text();
             if(repeated = (sField==sf && tField==tf)){
                bootGrowl("映射关系已存在.", "danger");
                return false;
             }
        });
        if(repeated){ return; }

        var index = $tr.size();
        var trHtml = "<tr title='双击设置/取消主键'><td>" + sField + "</td><td>" + tField + "</td><td></td><td><input id='fieldIndex_"+ (index + 1) +"' type='checkbox' class='fieldMappingDeleteCheckbox' /></td></tr>";
        $fieldMappingList.append(trHtml);

        initFieldMappingParams();
        bindFieldMappingListClick();
        bindFieldMappingCheckBoxClick();
        bindFieldMappingDelClick();
    });
}
// 绑定删除字段映射点击事件
function bindFieldMappingDelClick(){
    var $fieldMappingDelBtn = $("#fieldMappingDelBtn");
    $fieldMappingDelBtn.unbind("click");
    $fieldMappingDelBtn.click(function () {
        var ids = getCheckedBoxSize($('.fieldMappingDeleteCheckbox'));
        if (ids.length > 0) {
            var len = ids.length;
            for(i = 0; i < len; i++){
                $("#" + ids[i]).parent().parent().parent().remove();
            }
            $fieldMappingDelBtn.prop('disabled', true);
            initFieldMappingParams();
        }
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
    doLoader('/mapping/page/edit?id=' + $this.attr("mappingId"));
}

$(function() {
    // 绑定表字段关系点击事件
    initFieldMappingParams();
    // 绑定删除表字段映射事件
    bindFieldMappingCheckBoxClick();
    bindFieldMappingListClick();
    bindFieldMappingAddClick();
    bindFieldMappingDelClick();
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