//*********************************** 驱动保存 开始位置***********************************//
function submit(data) {
    doPoster("/mapping/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("修改驱动成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}
//*********************************** 驱动保存 结束位置***********************************//
// 刷新页面
function refresh(id){
    doLoader('/mapping/page/edit?id=' + id);
}

// 绑定修改驱动同步方式切换事件
function bindMappingModelChange() {
    var $mappingModelChange = $("#mappingModelChange");
    var $radio = $mappingModelChange.find('input:radio[type="radio"]');
    // 初始化icheck插件
    $radio.iCheck({
        labelHover: false,
        cursor: true,
        radioClass: 'iradio_flat-blue',
    }).on('ifChecked', function (event) {
        var $form = $("#mappingModifyForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            doPoster("/mapping/edit", data, function (response) {
                if (response.success == true) {
                    refresh($("#mappingId").val());
                } else {
                    bootGrowl(response.resultValue, "danger");
                }
            });
        }
    });

    // 渲染选择radio配置
    var $value = $mappingModelChange.find('input[type="radio"]:checked').val();
    // 显示驱动编辑配置（全量/增量）
    var $full = $("#mappingFullConfig");
    var $increment = $("#mappingIncrementConfig");
    if ('full' == $value) {
        $increment.addClass("hidden");
        $full.removeClass("hidden");
    } else {
        $full.addClass("hidden");
        $increment.removeClass("hidden");
    }
}

// 绑定删除表关系复选框事件
function bindMappingTableGroupCheckBoxClick(){
    var $checkboxAll = $('.tableGroupCheckboxAll');
    var $checkbox = $('.tableGroupCheckbox');
    var $delBtn = $("#tableGroupDelBtn");
    $checkboxAll.iCheck({
        checkboxClass: 'icheckbox_square-red',
        labelHover: false,
        cursor: true
    }).on('ifChecked', function (event) {
        $checkbox.iCheck('check');
    }).on('ifUnchecked', function (event) {
        $checkbox.iCheck('uncheck');
    }).on('ifChanged', function (event) {
        $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
    });

    // 初始化icheck插件
    $checkbox.iCheck({
        checkboxClass: 'icheckbox_square-red',
        cursor: true
    }).on('ifChanged', function (event) {
        $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
    });
}

// 获取选择的CheckBox[value]
function getCheckedBoxSize($checkbox){
    var checked = [];
    $checkbox.each(function(){
        if($(this).prop('checked')){
            checked.push($(this).val());
        }
    });
    return checked;
}

// 绑定表关系点击事件
function bindMappingTableGroupListClick() {
    var $tableGroupList = $("#tableGroupList");
    $tableGroupList.unbind("click");
    $tableGroupList.find("tr").bind('click', function () {
        doLoader('/tableGroup/page/editTableGroup?id=' + $(this).attr("id"));
    });
}

// 绑定下拉选择事件自动匹配相似表事件
function bindTableSelect(){
    var $sourceSelect = $("#sourceTable");
    var $targetSelect = $("#targetTable");
    $sourceSelect.on('changed.bs.select',function(e){
        $targetSelect.selectpicker('val', $(this).selectpicker('val'));
    });
    bindMappingTableGroupAddClick($sourceSelect, $targetSelect);
}

// 绑定新增表关系点击事件
function bindMappingTableGroupAddClick($sourceSelect, $targetSelect) {
    var $addBtn = $("#tableGroupAddBtn");
    $addBtn.unbind("click");
    $addBtn.bind('click', function () {
        var m = {};
        m.mappingId = $(this).attr("mappingId");
        m.sourceTable = $sourceSelect.selectpicker('val');
        m.targetTable = $targetSelect.selectpicker('val');
        if(undefined == m.sourceTable){
            bootGrowl("请选择数据源表", "danger");
            return;
        }
        if(undefined == m.targetTable){
            bootGrowl("请选择目标源表", "danger");
            return;
        }

        // 如果存在多个选择，只筛选相似表
        var sLen = m.sourceTable.length;
        var tLen = m.targetTable.length;
        if (1 < sLen || 1 < tLen) {
            var mark = [];
            for (j = 0; j < sLen; j++) {
                if (-1 != m.targetTable.indexOf(m.sourceTable[j])) {
                    mark.push(m.sourceTable[j]);
                }
            }
            m.sourceTable = mark;
            m.targetTable = mark;
        }
        m.sourceTable = m.sourceTable.join('|');
        m.targetTable = m.targetTable.join('|');

        doPoster("/tableGroup/add", m, function (data) {
            if (data.success == true) {
                bootGrowl("新增映射关系成功!", "success");
                refresh(m.mappingId);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

// 绑定删除表关系点击事件
function bindMappingTableGroupDelClick() {
    $("#tableGroupDelBtn").click(function () {
        var ids = getCheckedBoxSize($(".tableGroupCheckbox"));
        if (ids.length > 0) {
            var $mappingId = $(this).attr("mappingId");
            doPoster("/tableGroup/remove", {"mappingId": $mappingId, "ids" : ids.join()}, function (data) {
                if (data.success == true) {
                    bootGrowl("删除映射关系成功!", "success");
                    refresh($mappingId);
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });
}

// 修改驱动名称
function mappingModifyName(){
    var $name = $("#mappingModifyName");
    var tmp = $name.text();
    $name.text("");
    $name.append("<input type='text'/>");
    var $input = $name.find("input");
    $input.focus().val(tmp);
    $input.blur(function(){
        $name.text($(this).val());
        $("#mappingModifyForm input[name='name']").val($(this).val());
        $input.unbind();
    });
}

$(function () {
    // 绑定同步方式切换事件
    bindMappingModelChange();
    // 绑定删除表映射事件
    bindMappingTableGroupCheckBoxClick();

    // 绑定表关系点击事件
    bindMappingTableGroupListClick();
    // 绑定下拉选择事件自动匹配相似表事件
    bindTableSelect();
    // 绑定删除表关系点击事件
    bindMappingTableGroupDelClick();

    // 初始化select插件
    initSelectIndex($(".select-control-table"), 0);

    // 保存
    $("#mappingSubmitBtn").click(function () {
        var $form = $("#mappingModifyForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    // 返回
    $("#mappingBackBtn").click(function () {
        backIndexPage();
    });
})