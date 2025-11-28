
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
            "pk":($pk !== "" || $.trim($pk).length > 0)
        });
    });
    
    let $fieldMappingTable = $("#fieldMappingTable");
    if (0 >= row.length) {
        console.log('[初始化映射参数] 没有字段映射，隐藏表格');
        $fieldMappingTable.addClass("hidden");
    } else {
        console.log('[初始化映射参数] 找到', row.length, '个字段映射，显示表格');
        $fieldMappingTable.removeClass("hidden");
    }
    $("#fieldMapping").val(JSON.stringify(row));
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

// 获取选择的CheckBox[value]
function getCheckedBoxElements($checkbox){
    let checked = [];
    $checkbox.each(function(){
        if($(this).prop('checked')){
            // 修复：使用 closest('tr') 获取最近的 tr 元素
            // 原来的 parent().parent().parent() 会获取到 tbody，导致删除整个表格
            checked.push($(this).closest('tr'));
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
// 绑定删除表字段复选框事件（移除 iCheck 依赖）
function bindFieldMappingCheckBoxClick(){
    let $checkboxAll = $('.fieldMappingDeleteCheckboxAll');
    let $checkbox = $('.fieldMappingDeleteCheckbox');
    let $delBtn = $("#fieldMappingDelBtn");
    
    // 全选复选框事件
    $checkboxAll.on('change', function (event) {
        var isChecked = $(this).prop('checked');
        $checkbox.prop('checked', isChecked);
        $delBtn.prop('disabled', getCheckedBoxElements($checkbox).length < 1);
    });

    // 普通复选框事件
    $checkbox.on('change', function (event) {
        // 阻止tr触发click事件
        event.stopPropagation();
        event.cancelBubble = true;
        $delBtn.prop('disabled', getCheckedBoxElements($checkbox).length < 1);
        
        // 更新全选复选框状态
        var allChecked = $checkbox.length === $checkbox.filter(':checked').length;
        $checkboxAll.prop('checked', allChecked);
    });
    
    // 初始化删除按钮状态
    $delBtn.prop('disabled', getCheckedBoxElements($checkbox).length < 1);
}
// 绑定字段映射表格点击事件
function bindFieldMappingListClick(){
    // 行双击事件
    let $tr = $("#fieldMappingList tr");
    $tr.unbind("dblclick");
    $tr.bind('dblclick', function () {
        let $pk = $(this).find("td:eq(3)");
        let $text = $pk.html();
        let isPk = $text === "" || $.trim($text).length === 0;
        // 更新为新的图标样式
        $pk.html(isPk ? '<i title="主键" class="fa fa-key text-warning"></i>' : '');
        initFieldMappingParams();
    });
}
// 绑定下拉选择事件自动匹配相似字段事件
function bindTableFieldSelect(){
    let $sourceSelect = $("#sourceTableField");
    let $targetSelect = $("#targetTableField");
    
    // 增强 select 元素（替代 Bootstrap selectpicker）
    if (typeof enhanceSelect === 'function') {
        enhanceSelect($sourceSelect);
        enhanceSelect($targetSelect);
    }

    // 绑定数据源下拉切换事件（使用原生 change 事件）
    $sourceSelect.on('change',function(e){
        $targetSelect.val($(this).val());
    });
    bindFieldMappingAddClick($sourceSelect, $targetSelect)
}
// 绑定添加字段映射点击事件
function bindFieldMappingAddClick($sourceSelect, $targetSelect){
    let $btn = $("#fieldMappingAddBtn");
    $btn.bind('click', function(){
        let sField = $sourceSelect.val();
        let tField = $targetSelect.val();
        sField = sField == null ? "" : sField;
        tField = tField == null ? "" : tField;
        // 非空检查
        if(sField === "" && tField === ""){
            bootGrowl("至少有一个表字段.", "danger");
            return;
        }

        // 检查重复字段
        let repeated = false;
        let $fieldMappingList = $("#fieldMappingList");
        let $tr = $fieldMappingList.find("tr");
        $tr.each(function(k,v){
            let sf = $(this).find("td:eq(0)").text();
            let tf = $(this).find("td:eq(1)").text();
            if (repeated === (sField === sf && tField === tf)) {
                bootGrowl("映射关系已存在.", "danger");
                return false;
            }
        });
        if(repeated){ return; }

        let index = $tr.size();
        let trHtml = "<tr id='fieldMapping_"+ (index + 1) +"' title='双击设置/取消主键 | 拖动排序' class='dbsyncer_pointer' style='transition: background-color 0.2s ease;'>" +
                     "<td>" + sField + "</td>" +
                     "<td>" + tField + "</td>" +
                     "<td style='text-align: center;'></td>" +
                     "<td style='text-align: center;'><input type='checkbox' class='fieldMappingDeleteCheckbox' style='width: 16px; height: 16px; cursor: pointer;' /></td>" +
                     "</tr>";
        $fieldMappingList.append(trHtml);

        initFieldMappingParams();
        bindFieldMappingDrop();
        bindFieldMappingListClick();
        bindFieldMappingCheckBoxClick();
        bindFieldMappingDelClick();
    });
}
// 绑定删除字段映射点击事件
function bindFieldMappingDelClick(){
    let $fieldMappingDelBtn = $("#fieldMappingDelBtn");
    $fieldMappingDelBtn.unbind("click");
    $fieldMappingDelBtn.click(function () {
        let elements = getCheckedBoxElements($('.fieldMappingDeleteCheckbox'));
        if (elements.length > 0) {
            console.log('[删除字段映射] 删除', elements.length, '行');
            let len = elements.length;
            for(let i = 0; i < len; i++){
                elements[i].remove();
            }
            
            // 取消全选复选框的选中状态
            $('.fieldMappingDeleteCheckboxAll').prop('checked', false);
            
            // 禁用删除按钮
            $fieldMappingDelBtn.prop('disabled', true);
            
            // 更新映射参数（会自动显示/隐藏表格）
            initFieldMappingParams();
            
            console.log('[删除字段映射] 删除完成，剩余行数:', $("#fieldMappingList tr").length);
        }
    });
}

$(function() {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader("/mapping/page/edit?id=" + $("#mappingId").val());
    };

    // 绑定表字段关系点击事件
    initFieldMappingParams();
    // 绑定表格拖拽事件
    bindFieldMappingDrop();
    // 绑定下拉选择事件自动匹配相似字段事件
    bindTableFieldSelect();
    // 绑定刷新表字段事件
    bindRefreshTableFieldsClick();
    // 绑定删除表字段映射事件
    bindFieldMappingCheckBoxClick();
    bindFieldMappingListClick();
    bindFieldMappingDelClick();

    //保存
    $("#tableGroupSubmitBtn").click(function () {
        let $form = $("#table_group_modify_form");
        if (validateForm($form)) {
            //保存驱动配置
            doPoster("/tableGroup/edit", $form.serializeJson(), function (data) {
                if (data.success === true) {
                    bootGrowl("保存表映射关系成功!", "success");
                    backIndexPage();
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });
});