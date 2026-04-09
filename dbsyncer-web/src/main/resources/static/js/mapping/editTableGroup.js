// 缓存常用选择器
const $fieldMappingList = () => $("#fieldMappingList");
const $fieldCheckboxes = () => $('.fieldCheckbox');
const $fieldCheckboxAll = () => $('.fieldCheckboxAll');
const $fieldDelBtn = () => $('#fieldDelBtn');
const $fieldMapping = () => $("#fieldMapping");

function getFieldMappingTargetFromRow($tr) {
    const $sel = $tr.find("td:eq(2) select.field-mapping-target-nesting");
    if ($sel.length) {
        return $sel.val() || "";
    }
    return $tr.find("td:eq(2)").text().trim();
}

/** 数据源字段是否为 NESTING（嵌套），与 DataTypeEnum.NESTING 及 ES Nested 等兼容 */
function isSourceFieldNestingByName(sourceFieldName) {
    if (!sourceFieldName) {
        return false;
    }
    const tn = $("#source_table_field option").filter(function () {
        return $(this).val() === sourceFieldName;
    }).first().attr("data-type-name");
    if (!tn) {
        return false;
    }
    const u = String(tn).toUpperCase();
    return u === "RELTABLE";
}

// 初始化映射关系参数
function initFieldMappingParams(){
    // 生成JSON参数
    let row = [];
    const $list = $fieldMappingList();
    $list.find("tr").each(function(k,v){
        const $tr = $(this);
        row.push({
            "source": $tr.find("td:eq(1)").text().trim(),
            "target": getFieldMappingTargetFromRow($tr),
            "pk": $tr.find("td:eq(3) i").length > 0
        });
    });
    
    // 根据是否有数据来显示/隐藏表格（通过tbody的父元素table）
    const $table = $list.closest('table');
    if (row.length === 0) {
        $table.addClass("hidden");
    } else {
        $table.removeClass("hidden");
    }
    $fieldMapping().val(JSON.stringify(row));
}

function bindFieldSelect(selector, onChange, defaultValue){
    // 不传 defaultValue 时，保留 <select> 现有选中值（用于页面回显 th:selected）
    let dv = defaultValue;
    if (dv === undefined) {
        const current = selector.val();
        dv = current == null || current === "" ? [] : [current];
    }
    return selector.dbSelect({
        type: 'single',
        onSelect: function (data) {
            if (onChange) {
                onChange(data);
            }
        },
        defaultValue: dv,// 默认选中
        customButtons: [ // 最多2个自定义按钮
            {
                text: '刷新字段',
                callback: function(values) {
                    bindRefreshTableFieldsClick();
                }
            }
        ]
    });
}

// 绑定表格拖拽事件（使用原生 HTML5 拖拽 API）
function bindFieldMappingDrop() {
    const $list = $fieldMappingList();
    const rows = $list.find("tr");
    
    // 先移除旧的事件监听器，避免重复绑定
    rows.off('dragstart dragend dragover dragleave drop');
    rows.each(function() {
        const row = this;
        row.setAttribute('draggable', 'true');
    });
    
    // 使用事件委托，避免重复绑定
    $list.on('dragstart', 'tr', function(e) {
        e.originalEvent.dataTransfer.effectAllowed = 'move';
        e.originalEvent.dataTransfer.setData('text/html', this.innerHTML);
        $(this).addClass('dragging');
    });

    $list.on('dragend', 'tr', function(e) {
        $(this).removeClass('dragging');
        $list.find('tr').removeClass('drag-over');
        // 更新行号
        updateRowNumbers();
        initFieldMappingParams();
    });

    $list.on('dragover', 'tr', function(e) {
        e.preventDefault();
        e.originalEvent.dataTransfer.dropEffect = 'move';
        $(this).addClass('drag-over');
        return false;
    });

    $list.on('dragleave', 'tr', function(e) {
        $(this).removeClass('drag-over');
    });

    $list.on('drop', 'tr', function(e) {
        e.preventDefault();
        e.stopPropagation();
        
        const $dragging = $list.find('.dragging');
        if ($dragging.length && $dragging[0] !== this) {
            const draggingIndex = $dragging.index();
            const targetIndex = $(this).index();
            
            if (draggingIndex < targetIndex) {
                $(this).after($dragging);
            } else {
                $(this).before($dragging);
            }
        }
        
        $(this).removeClass('drag-over');
        return false;
    });
}

// 更新表格行号
function updateRowNumbers() {
    $fieldMappingList().find("tr").each(function(index) {
        $(this).find("td:eq(0)").text(index + 1);
    });
}

// 绑定复选框多选或单选事件
function bindFieldMappingCheckbox() {
    const $checkboxAll = $fieldCheckboxAll();
    const $delBtn = $fieldDelBtn();
    let isUpdatingSelectAll = false;

    // 更新全选复选框状态
    function updateSelectAllState() {
        if (isUpdatingSelectAll) return;
        isUpdatingSelectAll = true;
        const $checkboxes = $fieldCheckboxes();
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
        $delBtn.prop('disabled', $fieldCheckboxes().filter(':checked').length === 0);
    }

    // 先移除旧的事件绑定，避免重复绑定
    $checkboxAll.off('change');
    $fieldCheckboxes().off('change');

    // 初始化全选复选框
    $checkboxAll.checkboxGroup({
        theme: 'danger',
        size: 'md',
        onChange: function(values) {
            // 全选/取消全选时，同步所有单个复选框
            const isChecked = values.length > 0;
            $fieldCheckboxes().each(function() {
                const api = $(this).data('checkboxGroup');
                if (api) {
                    api.setValue(isChecked);
                }
            });
            updateDeleteButtonState();
        }
    });

    // 为已存在的复选框初始化
    $fieldCheckboxes().each(function() {
        if (!$(this).data('checkboxGroup')) {
            $(this).checkboxGroup({
                theme: 'danger',
                size: 'md',
                onChange: function(values) {
                    updateSelectAllState();
                    updateDeleteButtonState();
                }
            });
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
            bootGrowl(data.message, "danger");
        }
    });
}

// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    // 使用事件委托，支持动态添加的行
    const $list = $fieldMappingList();
    $list.off("dblclick", "tr").on('dblclick', 'tr', function () {
        const $pk = $(this).find("td:eq(3)");
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
        const $list = $fieldMappingList();
        const $tr = $list.find("tr");
        $tr.each(function (k, v) {
            const $row = $(this);
            let sf = $row.find("td:eq(1)").text().trim();
            let tf = getFieldMappingTargetFromRow($row);
            if (sField === sf && tField === tf) {
                repeated = true;
                return false; // 跳出循环
            }
        });
        if (repeated) {
            bootGrowl("字段映射已存在.", "danger");
            return;
        }

        const rowIndex = $tr.length + 1;
        const $newRow = $("<tr title='双击设置/取消主键 | 拖动排序'></tr>");
        $newRow.append($("<td></td>").text(rowIndex));
        $newRow.append($("<td></td>").text(sField));
        const $tdTarget = $("<td></td>");
        let $nestingSelectToInit = null;
        if (isSourceFieldNestingByName(sField)) {
            const $sel = $("#tableGroupChildOptionsCloneSource").clone();
            $sel.removeAttr("id").removeClass("hidden").removeAttr("aria-hidden").removeAttr("tabindex")
                .addClass("field-mapping-target-nesting").addClass("form-control").css("width", "100%");
            $tdTarget.append($sel);
            // 延后到挂载 DOM 后再初始化，否则可能出现下拉无数据
            $nestingSelectToInit = $sel;
        } else {
            $tdTarget.text(tField);
        }
        $newRow.append($tdTarget);
        $newRow.append($("<td></td>"));
        $newRow.append($("<td onclick=\"event.stopPropagation();\"></td>").append(
            $("<input type=\"checkbox\" class=\"fieldCheckbox\" onclick=\"event.stopPropagation();\" />")
        ));
        $list.append($newRow);
        if ($nestingSelectToInit) {
            bindFieldSelect($nestingSelectToInit, function () {
                initFieldMappingParams();
            });
        }

        initFieldMappingParams();
        bindFieldMappingDrop();
        bindFieldMappingListClick();
        bindFieldMappingCheckbox();
    });
}
// 绑定删除字段映射点击事件
function bindFieldMappingDelClick(){
    $fieldDelBtn().unbind("click").bind('click', function () {
        const checkedRows = [];
        $fieldCheckboxes().filter(':checked').each(function () {
            checkedRows.push($(this).closest('tr'));
        });
        if (checkedRows.length > 0) {
            // 删除选中的行
            checkedRows.forEach(function($row) {
                $row.remove();
            });
            
            // 更新行号
            updateRowNumbers();
            // 更新映射参数（会自动显示/隐藏表格）
            initFieldMappingParams();
            // 更新删除按钮状态
            $fieldDelBtn().prop('disabled', $fieldCheckboxes().filter(':checked').length === 0);
        }
    });
}

$(function() {
    // 定义返回函数，子页面返回
    let mappingId = $("#mappingId").val();
    window.backIndexPage = function () {
        doLoader("/mapping/page/edit?id=" + mappingId);
    };

    // 绑定下拉选择事件自动匹配相似字段事件
    let targetTableSelect = bindFieldSelect($('#target_table_field'));
    let sourceTableSelect = bindFieldSelect($('#source_table_field'), function(data) {
        targetTableSelect.setValues(data);
    });
    
    // 初始化页面已存在的“关联表(嵌套)”下拉为可搜索组件（Thymeleaf 渲染的行）
    $("select.field-mapping-target-nesting").each(function () {
        const $sel = $(this);
        if ($sel.data("dbSelectInited")) {
            return;
        }
        $sel.data("dbSelectInited", true);
        bindFieldSelect($sel, function () {
            initFieldMappingParams();
        });
    });

    // 绑定表字段关系点击事件
    initFieldMappingParams();
    // 嵌套目标下拉变更时同步隐藏域
    $fieldMappingList().off("change", "select.field-mapping-target-nesting").on("change", "select.field-mapping-target-nesting", function () {
        initFieldMappingParams();
    });
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
                    bootGrowl(data.message, "danger");
                }
            });
        }
    });
});