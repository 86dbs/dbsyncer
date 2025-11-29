
// 初始化映射关系参数
function initFieldMappingParams(){
    // 生成JSON参数
    let row = [];
    $("#fieldMappingList").find("tr").each(function(k,v){
        row.push({
            "source":$(this).find("td:eq(1)").text(),
            "target":$(this).find("td:eq(2)").text(),
            "pk": $(this).find("td:eq(3) i").length > 0
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

function bindFieldSelect(selector, onChange){
    return selector.dbSelect({
        type: 'single',
        onSelect: function (data) {
            if (onChange) {
                onChange(data);
            }
        },
        defaultValue: [],// 默认选中
        customButtons: [ // 最多2个自定义按钮
            {
                text: '刷新字段',
                callback: function(values) {
                    bindRefreshTableFieldsClick();
                }
            }
        ],
        onCustomButton: function(index, values, text) {  // 自定义按钮点击
            console.log('按钮索引:', index, '选中值:', values, '按钮文本:', text);
        }
    });
}

// 绑定表格拖拽事件（使用原生 HTML5 拖拽 API）
function bindFieldMappingDrop() {
    var $tableBody = $("#fieldMappingList");
    var rows = $tableBody.find("tr");
    
    rows.each(function() {
        var row = $(this)[0];
        row.setAttribute('draggable', 'true');
        
        row.addEventListener('dragstart', handleDragStart);
        row.addEventListener('dragend', handleDragEnd);
        row.addEventListener('dragover', handleDragOver);
        row.addEventListener('dragleave', handleDragLeave);
        row.addEventListener('drop', handleDrop);
    });
    
    var dragSrcEl = null;
    
    function handleDragStart(e) {
        dragSrcEl = this;
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/html', this.innerHTML);
        this.classList.add('dragging');
    }
    
    function handleDragEnd(e) {
        this.classList.remove('dragging');
        var rows = $tableBody.find('tr');
        rows.removeClass('drag-over');
        initFieldMappingParams();
    }
    
    function handleDragOver(e) {
        if (e.preventDefault) {
            e.preventDefault();
        }
        e.dataTransfer.dropEffect = 'move';
        return false;
    }
    
    function handleDragLeave(e) {
        this.classList.remove('drag-over');
    }
    
    function handleDrop(e) {
        if (e.stopPropagation) {
            e.stopPropagation();
        }
        
        if (dragSrcEl !== this) {
            var srcIndex = $(dragSrcEl).index();
            var destIndex = $(this).index();
            
            if (srcIndex < destIndex) {
                $(this).after(dragSrcEl);
            } else {
                $(this).before(dragSrcEl);
            }
        }
        
        this.classList.remove('drag-over');
        return false;
    }
}

// 绑定复选框多选或单选事件
function bindFieldMappingCheckbox() {
    const $checkboxAll = $('.fieldCheckboxAll');
    const $checkboxes = $('.fieldCheckbox');
    const $delBtn = $('#fieldDelBtn');
    let isUpdatingSelectAll = false;

    // 更新全选复选框状态
    function updateSelectAllState() {
        if (isUpdatingSelectAll) return;
        isUpdatingSelectAll = true;
        const $checkedCheckboxes = $checkboxes.filter(':checked');
        const allChecked = $checkboxes.length > 0 && $checkboxes.length === $checkedCheckboxes.length;
        const checkboxAllApi = $checkboxAll.data('checkboxGroup');
        if (checkboxAllApi) {
            checkboxAllApi.setValue(allChecked);
        } else {
            $checkboxAll.prop('checked', allChecked);
        }
        setTimeout(function() {
            isUpdatingSelectAll = false;
        }, 0);
    }

    // 更新删除按钮状态
    function updateDeleteButtonState() {
        $delBtn.prop('disabled', $checkboxes.filter(':checked').length === 0);
    }

    // 初始化全选复选框
    $checkboxAll.checkboxGroup({
        theme: 'danger',
        size: 'md',
        onChange: function(values) {
            // 全选/取消全选时，同步所有单个复选框
            const isChecked = values.length > 0;
            $checkboxes.each(function() {
                const api = $(this).data('checkboxGroup');
                if (api) {
                    api.setValue(isChecked);
                }
            });
            updateDeleteButtonState();
        }
    });

    // 初始化所有单个复选框
    $checkboxes.checkboxGroup({
        theme: 'danger',
        size: 'md',
        onChange: function(values) {
            updateSelectAllState();
            updateDeleteButtonState();
        }
    });
}

// 绑定刷新表字段事件
function bindRefreshTableFieldsClick() {
    let id = $("#tableGroupId").val();
    doPoster("/tableGroup/refreshFields", {'id': id}, function (data) {
        if (data.success === true) {
            bootGrowl("刷新字段成功!", "success");
            doLoader('/tableGroup/page/editTableGroup?id=' + id);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    // 行双击事件
    $("#fieldMappingList tr").unbind("dblclick").bind('dblclick', function () {
        let $pk = $(this).find("td:eq(3)");
        // 更新为新的图标样式
        $pk.html($pk.find("i").length > 0 ? '' : '<i title="主键" class="fa fa-key text-warning"></i>');
        initFieldMappingParams();
    });
}

// 绑定添加字段映射点击事件
function bindFieldMappingAddClick(sourceSelector, targetSelector){
    $("#fieldAddBtn").unbind("click").bind('click', function(){
        let sValues = sourceSelector.getValues();
        let tValues = targetSelector.getValues();
        // 非空检查
        if(sValues.length < 1 && tValues.length < 1){
            bootGrowl("至少有一个字段.", "danger");
            return;
        }

        // 检查重复字段
        let sField = sValues[0];
        let tField = tValues[0];
        sField = sField == null ? "" : sField;
        tField = tField == null ? "" : tField;
        let repeated = false;
        let fieldMappingList = $("#fieldMappingList");
        let $tr = fieldMappingList.find("tr");
        $tr.each(function (k, v) {
            let sf = $(this).find("td:eq(1)").text();
            let tf = $(this).find("td:eq(2)").text();
            if (sField === sf && tField === tf) {
                repeated = true;
                return false; // 跳出循环
            }
        });
        if (repeated) {
            bootGrowl("字段映射已存在.", "danger");
            return;
        }

        fieldMappingList.append(`
                <tr title='双击设置/取消主键 | 拖动排序'>
                    <td>${($tr.size() + 1)}</td>
                    <td>${sField}</td>
                    <td>${tField}</td>
                    <td></td>
                    <td onclick="event.stopPropagation();">
                        <input type="checkbox" class="fieldCheckbox" onclick="event.stopPropagation();" />
                    </td>
                </tr>`);
        bootGrowl("添加字段映射.", "success");

        initFieldMappingParams();
        bindFieldMappingDrop();
        bindFieldMappingListClick();
        bindFieldMappingCheckbox();
        bindFieldMappingRemove();
    });
}
// 绑定删除字段映射点击事件
function bindFieldMappingDelClick(){
    $("#fieldDelBtn").unbind("click").bind('click', function () {
        let checked = [];
        $(".fieldCheckbox").each(function () {
            if ($(this).prop('checked')) {
                $(this).closest('tr').remove();
                checked.push(1);
            }
        });
        if (checked.length > 0) {
            // 更新映射参数（会自动显示/隐藏表格）
            initFieldMappingParams();
            $(this).prop('disabled', true);
            bootGrowl("删除字段映射.", "success");
        }
    });
}

$(function() {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader("/mapping/page/edit?id=" + $("#mappingId").val());
    };

    // 绑定下拉选择事件自动匹配相似字段事件
    let targetTableSelect = bindFieldSelect($('#target_table_field'));
    let sourceTableSelect = bindFieldSelect($('#source_table_field'), function(data) {
        targetTableSelect.setValues(data);
    });

    // 绑定表字段关系点击事件
    initFieldMappingParams();
    // 绑定表格拖拽事件
    bindFieldMappingDrop();
    // 绑定删除表字段映射事件
    bindFieldMappingCheckbox();
    // 绑定添加字段映射关系事件
    bindFieldMappingAddClick(sourceTableSelect, targetTableSelect);
    // 绑定删除字段映射关系事件
    bindFieldMappingDelClick();
    // 绑定字段关系点击事件
    bindFieldMappingListClick();

    //保存
    $("#tableGroupSubmitBtn").click(function () {
        let $form = $("#table_group_modify_form");
        if (validateForm($form)) {
            //保存驱动配置
            doPoster("/tableGroup/edit", $form.serializeJson(), function (data) {
                if (data.success === true) {
                    bootGrowl("修改表字段映射成功!", "success");
                    backIndexPage();
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });
});