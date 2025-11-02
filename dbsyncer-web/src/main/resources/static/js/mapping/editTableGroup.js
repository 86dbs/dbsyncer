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
        if(sField == "" && tField == ""){
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
            if (repeated = (sField == sf && tField == tf)) {
                bootGrowl("映射关系已存在.", "danger");
                return false;
            }
        });
        if(repeated){ return; }

        let index = $tr.size();
        let trHtml = "<tr id='fieldMapping_"+ (index + 1) +"' title='双击设置/取消主键'><td>" + sField + "</td><td>" + tField + "</td><td></td><td><input type='checkbox' class='fieldMappingDeleteCheckbox' /></td></tr>";
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
    doLoader('/mapping/page/edit?id=' + $this.attr("mappingId"));
}

$(function() {
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
        let $form = $("#tableGroupModifyForm");
        if ($form.formValidate() == true) {
            submit($form.serializeJson());
        }
    });

    // 返回按钮，跳转至上个页面
    $("#tableGroupBackBtn").bind('click', function(){
        backMappingPage($(this));
    });
});