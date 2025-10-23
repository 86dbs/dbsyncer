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

// 初始化映射关系参数
function initFieldMappingParams(){
    // 生成JSON参数
    let row = [];
    let $fieldMappingList = $("#fieldMappingList");
    $fieldMappingList.find("tr").each(function(k,v){
        let $pk = $(this).find("td:eq(2)").html();
        row.push({
            "source":$(this).find("td:eq(0)").text(),
            "target":$(this).find("td:eq(1)").text(),
            "pk":($pk != "" || $.trim($pk).length > 0)
        });
    });
    let $fieldMappingTable = $("#fieldMappingTable");
    if (0 >= row.length) {
        $fieldMappingTable.addClass("hidden");
    } else {
        $fieldMappingTable.removeClass("hidden");
    }
    $("#fieldMapping").val(JSON.stringify(row));
}

// 绑定表格拖拽事件
function bindFieldMappingDrop() {
    $("#fieldMappingList").tableDnD({
        onDrop: function(table, row) {
            initFieldMappingParams();
        }
    });
}

// 获取选择的CheckBox[value]
function getCheckedBoxElements($checkbox){
    let checked = [];
    $checkbox.each(function(){
        if($(this).prop('checked')){
            checked.push($(this).parent().parent().parent());
        }
    });
    return checked;
}
// 绑定刷新表字段事件
function bindRefreshTableFieldsClick() {
    let $refreshBtn = $("#refreshTableFieldBtn");
    $refreshBtn.bind('click', function(){
        let id = $(this).attr("tableGroupId");
        doPoster("/tableGroup/refreshFields", {'id': id}, function (data) {
            if (data.success == true) {
                bootGrowl("刷新字段成功!", "success");
                doLoader('/tableGroup/page/editTableGroup?id=' + id);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}
// 绑定删除表字段复选框事件
function bindFieldMappingCheckBoxClick(){
    let $checkboxAll = $('.fieldMappingDeleteCheckboxAll');
    let $checkbox = $('.fieldMappingDeleteCheckbox');
    let $delBtn = $("#fieldMappingDelBtn");
    $checkboxAll.iCheck({
        checkboxClass: 'icheckbox_square-red',
        labelHover: false,
        cursor: true
    }).on('ifChecked', function (event) {
        $checkbox.iCheck('check');
    }).on('ifUnchecked', function (event) {
        $checkbox.iCheck('uncheck');
    }).on('ifChanged', function (event) {
        $delBtn.prop('disabled', getCheckedBoxElements($checkbox).length < 1);
    });

    // 初始化icheck插件
    $checkbox.iCheck({
        checkboxClass: 'icheckbox_square-red',
        cursor: true
    }).on('ifChanged', function (event) {
        // 阻止tr触发click事件
        event.stopPropagation();
        event.cancelBubble=true;
        $delBtn.prop('disabled', getCheckedBoxElements($checkbox).length < 1);
    });
}
// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    // 行双击事件
    let $tr = $("#fieldMappingList tr");
    $tr.unbind("dblclick");
    $tr.bind('dblclick', function () {
        let $pk = $(this).find("td:eq(2)");
        let $text = $pk.html();
        let isPk = $text == "" || $.trim($text).length == 0;
        $pk.html(isPk ? '<i title="主键" class="fa fa-key fa-fw fa-rotate-90 text-warning"></i>' : '');
        initFieldMappingParams();
    });
}
// 绑定下拉选择事件自动匹配相似字段事件
function bindTableFieldSelect(){
    let $sourceSelect = $("#sourceTableField");
    let $targetSelect = $("#targetTableField");

    // 绑定数据源下拉切换事件
    $sourceSelect.on('changed.bs.select',function(e){
        $targetSelect.selectpicker('val', $(this).selectpicker('val'));
    });
    
    bindFieldMappingAddClick($sourceSelect, $targetSelect);
}

// 绑定下拉过滤按钮点击事件
function bindFieldSelectFilterBtnClick() {
    // 查找操作按钮容器
    var $sourceActionsBox = findActionsBox('#sourceTableField');
    var $targetActionsBox = findActionsBox('#targetTableField');
    
    // 添加过滤按钮
    if ($sourceActionsBox && $sourceActionsBox.length > 0) {
        addFilterButtons($sourceActionsBox, 'source');
    }
    
    if ($targetActionsBox && $targetActionsBox.length > 0) {
        addFilterButtons($targetActionsBox, 'target');
    }
    
    // 绑定事件（使用事件委托）
    $(document).off('click', '.bs-show-all-source, .bs-exclude-all-source, .bs-show-all-target, .bs-exclude-all-target');
    
    $(document).on('click', '.bs-show-all-source', function (e) {
        e.preventDefault();
        window.refreshFieldsWithFilter('source', false);
        bootGrowl("取消过滤成功!", "success");
    });
    
    $(document).on('click', '.bs-exclude-all-source', function (e) {
        e.preventDefault();
        window.refreshFieldsWithFilter('source', true);
        bootGrowl("过滤成功!", "success");
    });
    
    $(document).on('click', '.bs-show-all-target', function (e) {
        e.preventDefault();
        window.refreshFieldsWithFilter('target', false);
        bootGrowl("取消过滤成功!", "success");
    });
    
    $(document).on('click', '.bs-exclude-all-target', function (e) {
        e.preventDefault();
        window.refreshFieldsWithFilter('target', true);
        bootGrowl("过滤成功!", "success");
    });
}

// 查找操作按钮容器的辅助函数
function findActionsBox(selectId) {
    var $element = $(selectId);
    
    // 方法1：查找同级的bootstrap-select容器内的bs-actionsbox
    var $actionsBox = $element.siblings('.bootstrap-select').find('.bs-actionsbox .btn-group');
    if ($actionsBox.length > 0) {
        return $actionsBox;
    }
    
    // 方法2：查找父级容器内的bs-actionsbox
    $actionsBox = $element.parent().find('.bs-actionsbox .btn-group');
    if ($actionsBox.length > 0) {
        return $actionsBox;
    }
    
    // 方法3：查找整个文档中对应的bs-actionsbox（通过data-id匹配）
    var selectElement = $element[0];
    if (selectElement) {
        var selectId = selectElement.id;
        $actionsBox = $('[aria-owns*="' + selectId + '"]').siblings('.dropdown-menu').find('.bs-actionsbox .btn-group');
        if ($actionsBox.length > 0) {
            return $actionsBox;
        }
    }
    
    // 方法4：直接查找页面中的bs-actionsbox（作为最后手段）
    $actionsBox = $('.bs-actionsbox .btn-group');
    if ($actionsBox.length > 0) {
        return $actionsBox.first(); // 只返回第一个
    }
    
    return null;
}

// 添加过滤按钮的辅助函数
function addFilterButtons($actionsBox, fieldType) {
    // 检查是否已经添加过按钮
    var existingButtons = $actionsBox.find('.bs-show-all-' + fieldType + ', .bs-exclude-all-' + fieldType);
    if (existingButtons.length > 0) {
        return;
    }
    
    // 添加过滤按钮（使用双重事件机制：事件委托 + inline onclick 确保可靠性）
    $actionsBox.append(
        '<button type="button" class="actions-btn bs-show-all-' + fieldType + ' btn btn-default" title="显示所有字段，包含已添加的字段" onclick="window.refreshFieldsWithFilter(\'' + fieldType + '\', false);">取消过滤</button>' +
        '<button type="button" class="actions-btn bs-exclude-all-' + fieldType + ' btn btn-default" title="不显示已添加的字段" onclick="window.refreshFieldsWithFilter(\'' + fieldType + '\', true);">过滤</button>'
    );
    
    // 调整按钮宽度为20%
    $actionsBox.find('.actions-btn').css('width', '20%');
}

// 刷新字段并应用过滤（设置为全局函数，供内联事件调用）
window.refreshFieldsWithFilter = function(fieldType, excludeAdded) {
    // 获取已添加的字段映射
    let addedFields = [];
    let $fieldMappingList = $("#fieldMappingList");
    
    $fieldMappingList.find("tr").each(function(){
        if (fieldType === 'source') {
            let sourceField = $(this).find("td:eq(0)").text().trim();
            if (sourceField) {
                addedFields.push(sourceField);
            }
        } else {
            let targetField = $(this).find("td:eq(1)").text().trim();
            if (targetField) {
                addedFields.push(targetField);
            }
        }
    });
    
    // 获取对应的select元素
    let $select = fieldType === 'source' ? $("#sourceTableField") : $("#targetTableField");
    
    // 使用Bootstrap Select的方式过滤选项
    $select.find('option').each(function() {
        let $option = $(this);
        let optionValue = $option.val();
        let isAdded = addedFields.includes(optionValue);
        
        if (excludeAdded && isAdded) {
            // 禁用并隐藏选项
            $option.prop('disabled', true);
            $option.attr('data-hidden', 'true');
        } else {
            // 启用并显示选项
            $option.prop('disabled', false);
            $option.removeAttr('data-hidden');
        }
    });
    
    // 重新初始化selectpicker以应用变更
    $select.selectpicker('destroy').selectpicker({
        "title": "请选择",
        "actionsBox": true,
        "liveSearch": true,
        "selectAllText": "全选",
        "deselectAllText": "取消全选",
        "noneResultsText": "没有找到 {0}",
        "selectedTextFormat": "count > 10"
    });
    
    // 重新绑定过滤按钮（因为重新初始化后按钮会消失）
    setTimeout(function() {
        if (fieldType === 'source') {
            var $actionsBox = findActionsBox('#sourceTableField');
            // 只在按钮不存在时才添加，避免重复添加
            if ($actionsBox && $actionsBox.find('.bs-show-all-source').length === 0) {
                addFilterButtons($actionsBox, 'source');
            }
        } else {
            var $actionsBox = findActionsBox('#targetTableField');
            // 只在按钮不存在时才添加，避免重复添加
            if ($actionsBox && $actionsBox.find('.bs-show-all-target').length === 0) {
                addFilterButtons($actionsBox, 'target');
            }
        }
    }, 100);
}
// 检查字段是否为主键
function isSourceFieldPrimaryKey(fieldName) {
    let $sourceSelect = $("#sourceTableField");
    let $option = $sourceSelect.find('option[value="' + fieldName + '"]');
    return $option.length > 0 && $option.attr('data-pk') === 'true';
}

// 绑定添加字段映射点击事件
function bindFieldMappingAddClick($sourceSelect, $targetSelect){
    let $btn = $("#fieldMappingAddBtn");
    $btn.bind('click', function(){
        let sFields = $sourceSelect.selectpicker("val");
        let tFields = $targetSelect.selectpicker("val");
        sFields = sFields == null ? [] : (Array.isArray(sFields) ? sFields : [sFields]);
        tFields = tFields == null ? [] : (Array.isArray(tFields) ? tFields : [tFields]);
        
        // 非空检查
        if(sFields.length == 0 && tFields.length == 0){
            bootGrowl("至少选择一个表字段.", "danger");
            return;
        }

        // 如果未选择目标字段，则使用源字段作为目标字段
        if(sFields.length > 0 && tFields.length == 0){
            tFields = [...sFields];
        }

        // 如果未选择源字段，则使用目标字段作为源字段
        if(sFields.length == 0 && tFields.length > 0){
            sFields = [...tFields];
        }
        
        // 检查数量是否一致
        if(sFields.length != tFields.length){
            bootGrowl("源字段和目标字段数量不一致，请检查选择.", "danger");
            return;
        }

        let $fieldMappingList = $("#fieldMappingList");
        let $tr = $fieldMappingList.find("tr");
        let addedCount = 0;
        
        // 批量添加字段映射
        for(let i = 0; i < sFields.length; i++){
            let sField = sFields[i];
            let tField = tFields[i];
            
            // 检查重复字段
            let repeated = false;
            $tr.each(function(k,v){
                let sf = $(this).find("td:eq(0)").text();
                let tf = $(this).find("td:eq(1)").text();
                if (repeated = (sField == sf && tField == tf)) {
                    return false;
                }
            });
            
            if(!repeated){
                let index = $tr.size() + addedCount + 1;
                
                // 检查源字段是否为主键，如果是则自动标记为主键
                let isPrimaryKey = isSourceFieldPrimaryKey(sField);
                let pkIcon = isPrimaryKey ? '<i title="主键" class="fa fa-key fa-fw fa-rotate-90 text-warning"></i>' : '';
                
                let trHtml = "<tr id='fieldMapping_"+ index +"' title='双击设置/取消主键'><td>" + sField + "</td><td>" + tField + "</td><td>" + pkIcon + "</td><td><input type='checkbox' class='fieldMappingDeleteCheckbox' /></td></tr>";
                $fieldMappingList.append(trHtml);
                addedCount++;
            }
        }
        
        if(addedCount > 0){
            // 统计自动标记为主键的字段数量
            let autoPkCount = 0;
            for(let i = 0; i < sFields.length; i++){
                if(isSourceFieldPrimaryKey(sFields[i])){
                    autoPkCount++;
                }
            }
            
            let message = "成功添加 " + addedCount + " 个字段映射关系!";
            if(autoPkCount > 0){
                message += " 其中 " + autoPkCount + " 个源主键字段已自动标记为主键。";
            }
            bootGrowl(message, "success");
            
            initFieldMappingParams();
            bindFieldMappingDrop();
            bindFieldMappingListClick();
            bindFieldMappingCheckBoxClick();
            bindFieldMappingDelClick();
            
            // 清空选择
            $sourceSelect.selectpicker('deselectAll');
            $targetSelect.selectpicker('deselectAll');
        } else {
            bootGrowl("没有新的映射关系需要添加.", "info");
        }
    });
}
// 绑定删除字段映射点击事件
function bindFieldMappingDelClick(){
    let $fieldMappingDelBtn = $("#fieldMappingDelBtn");
    $fieldMappingDelBtn.unbind("click");
    $fieldMappingDelBtn.click(function () {
        let elements = getCheckedBoxElements($('.fieldMappingDeleteCheckbox'));
        if (elements.length > 0) {
            let len = elements.length;
            for(let i = 0; i < len; i++){
                elements[i].remove();
            }
            $fieldMappingDelBtn.prop('disabled', true);
            initFieldMappingParams();
        }
    });
}
// 返回驱动配置页面
function backMappingPage($this){
    // 返回接口时增加classOn=1，edit页面接到后默认展开映射关系tab
    doLoader('/mapping/page/edit?classOn=1&id=' + $this.attr("mappingId"));
}

function doTableGroupSubmit() {
   //保存
   let $form = $("#tableGroupModifyForm");
   if ($form.formValidate() == true) {
       submit($form.serializeJson());
   }
}

$(function() {
    // 初始化select插件
    initSelectIndex($(".select-control-table"), -1);
    
    // 绑定表字段关系点击事件
    initFieldMappingParams();
    // 绑定表格拖拽事件
    bindFieldMappingDrop();
    // 绑定下拉选择事件自动匹配相似字段事件
    bindTableFieldSelect();
    
    // 延迟初始化过滤按钮，等待 selectpicker 完全初始化
    setTimeout(function() {
        // 只在没有按钮时才执行绑定，避免重复绑定
        if ($('.bs-show-all-source').length === 0 && $('.bs-show-all-target').length === 0) {
            bindFieldSelectFilterBtnClick();
        }
    }, 1000); // 增加到1秒
    
    // 绑定刷新表字段事件
    bindRefreshTableFieldsClick();
    // 绑定删除表字段映射事件
    bindFieldMappingCheckBoxClick();
    bindFieldMappingListClick();
    bindFieldMappingDelClick();



    // 返回按钮，跳转至上个页面
    $("#tableGroupBackBtn").bind('click', function(){
        backMappingPage($(this));
    });
});