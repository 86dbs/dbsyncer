//*********************************** 驱动保存 开始位置***********************************//
function submit(data) {
    doPoster("/mapping/edit", data, function (response) {
        if (response.success == true) {
            bootGrowl("修改驱动成功!", "success");
            // 优先返回到同步任务管理页面，如果该函数不存在则返回到默认主页
            if (typeof loadMappingListPage === 'function') {
                // 返回到同步任务管理页面
                loadMappingListPage();
            } else if (typeof backIndexPage === 'function') {
                // 返回到默认主页
                backIndexPage();
            } else {
                // 最后的兜底方案：使用doLoader加载同步任务列表
                if (typeof doLoader === 'function') {
                    doLoader('/mapping/list', 0);
                } else {
                    // 如果都不存在，直接跳转
                    window.location.href = '/mapping/list';
                }
            }
        } else {
            bootGrowl(response.resultValue, "danger");
        }
    });
}
//*********************************** 驱动保存 结束位置***********************************//
// 刷新页面
function refresh(id){
    doLoader('/mapping/page/edit?id=' + id);
}

// 绑定修改驱动同步方式切换事件（移除 iCheck 依赖）
function bindMappingModelChange() {


    var $mappingModelChange = $("#mappingModelChange");
    var $radio = $mappingModelChange.find('input:radio[type="radio"]');

    console.log($radio)

    // 使用原生 change 事件替代 iCheck
    $radio.on('change', function (event) {


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

// 绑定删除表关系复选框事件（移除 iCheck 依赖）
function bindMappingTableGroupCheckBoxClick(){
    var $checkboxAll = $('.tableGroupCheckboxAll');
    var $checkbox = $('.tableGroupCheckbox');
    var $delBtn = $("#tableGroupDelBtn");
    
    // 检查元素是否存在
    if (!$checkboxAll.length || !$checkbox.length || !$delBtn.length) {
        return;
    }
    
    // 阻止复选框点击事件冒泡
    $checkboxAll.on('click', function (event) {
        event.stopPropagation();
    });
    
    $checkbox.on('click', function (event) {
        event.stopPropagation();
    });
    
    // 全选复选框事件
    $checkboxAll.on('change', function (event) {
        var isChecked = $(this).prop('checked');
        $checkbox.prop('checked', isChecked);
        $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
    });

    // 单个复选框事件
    $checkbox.on('change', function (event) {
        // 更新全选复选框状态
        var allChecked = $checkbox.length === $checkbox.filter(':checked').length;
        $checkboxAll.prop('checked', allChecked);
        
        // 更新删除按钮状态
        $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
    });
    
    // 初始化删除按钮状态
    $delBtn.prop('disabled', getCheckedBoxSize($checkbox).length < 1);
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

// 绑定表关系点击事件（移除 tableDnD 依赖）
function bindMappingTableGroupListClick() {
    var $tableGroupList = $("#tableGroupList");
    
    // 检查元素是否存在
    if (!$tableGroupList.length) {
        return;
    }
    
    $tableGroupList.unbind("click");
    $tableGroupList.find("tr").bind('click', function (e) {
        // 如果点击的是复选框或其父元素的td，不执行跳转
        if ($(e.target).is('input[type="checkbox"]') || $(e.target).closest('td').find('input[type="checkbox"]').length > 0) {
            return;
        }
        doLoader('/tableGroup/page/editTableGroup?id=' + $(this).attr("id"));
    });

    // 使用原生 HTML5 拖拽 API 替代 tableDnD
    var $rows = $tableGroupList.find("tr");
    $rows.each(function() {
        var $row = $(this);
        $row.attr('draggable', 'true');
        
        // 拖拽开始
        $row.on('dragstart', function(e) {
            e.originalEvent.dataTransfer.effectAllowed = 'move';
            e.originalEvent.dataTransfer.setData('text/html', this.innerHTML);
            $(this).addClass('dragging');
        });
        
        // 拖拽结束
        $row.on('dragend', function(e) {
            $(this).removeClass('dragging');
            $rows.removeClass('drag-over');
            
            // 更新排序数据
            var newData = [];
            $tableGroupList.find("tr").each(function () {
                newData.push($(this).attr('id'));
            });
            $("#sortedTableGroupIds").val(newData.join('|'));
        });
        
        // 拖拽经过
        $row.on('dragover', function(e) {
            if (e.preventDefault) {
                e.preventDefault();
            }
            e.originalEvent.dataTransfer.dropEffect = 'move';
            $(this).addClass('drag-over');
            return false;
        });
        
        // 拖拽离开
        $row.on('dragleave', function(e) {
            $(this).removeClass('drag-over');
        });
        
        // 放置
        $row.on('drop', function(e) {
            if (e.stopPropagation) {
                e.stopPropagation();
            }
            
            var $dragging = $tableGroupList.find('.dragging');
            if ($dragging.length && $dragging[0] !== this) {
                // 判断插入位置
                var draggingIndex = $dragging.index();
                var targetIndex = $(this).index();
                
                if (draggingIndex < targetIndex) {
                    $(this).after($dragging);
                } else {
                    $(this).before($dragging);
                }
            }
            
            return false;
        });
    });
}

// 绑定下拉选择事件自动匹配相似表事件（使用新的多选下拉框）
function bindTableSelect(){
    console.log("[bindTableSelect] 开始绑定表选择器");
    
    const $sourceSelect = $("#sourceTable");
    const $targetSelect = $("#targetTable");
    const $sourceWrapper = $("#sourceTableWrapper");
    const $targetWrapper = $("#targetTableWrapper");
    
    console.log("[bindTableSelect] 元素检查:", {
        sourceSelect: $sourceSelect.length,
        targetSelect: $targetSelect.length,
        sourceWrapper: $sourceWrapper.length,
        targetWrapper: $targetWrapper.length
    });
    
    // 检查元素是否存在
    if (!$sourceSelect.length || !$targetSelect.length) {
        console.warn('[bindTableSelect] 表选择器元素未找到');
        return;
    }
    
    // 初始化多选下拉框
    if (typeof initMultiSelect === 'function') {
        // 检查包装器是否存在
        if ($sourceWrapper.length) {
            console.log("[bindTableSelect] 初始化数据源表选择器");
            // 初始化数据源表选择器，联动到目标源表
            initMultiSelect('sourceTableWrapper', {
                linkTo: 'targetTableWrapper',
                onSelectChange: function() {
                    console.log("[bindTableSelect] 数据源表选择变化");
                }
            });
        } else {
            console.warn("[bindTableSelect] sourceTableWrapper 元素未找到");
        }
        
        if ($targetWrapper.length) {
            console.log("[bindTableSelect] 初始化目标源表选择器");
            // 初始化目标源表选择器
            initMultiSelect('targetTableWrapper', {
                linkTo: null,
                onSelectChange: function() {
                    console.log("[bindTableSelect] 目标源表选择变化");
                }
            });
        } else {
            console.warn("[bindTableSelect] targetTableWrapper 元素未找到");
        }
    } else {
        console.error("[bindTableSelect] initMultiSelect 函数未定义");
    }
    
    bindMappingTableGroupAddClick($sourceSelect, $targetSelect);
    console.log("[bindTableSelect] 表选择器绑定完成");
}

// 绑定下拉过滤按钮点击事件
// 绑定过滤按钮（新的多选下拉框已内置此功能）
function bindMultipleSelectFilterBtnClick() {
    // 新的多选下拉框已经内置了"取消过滤"和"过滤"按钮
    // 无需额外添加，但保留此函数以向后兼容
}

// 绑定新增表关系点击事件
function bindMappingTableGroupAddClick($sourceSelect, $targetSelect) {
    let $addBtn = $("#tableGroupAddBtn");
    
    // 检查按钮是否存在
    if (!$addBtn.length) {
        return;
    }
    
    $addBtn.unbind("click");
    $addBtn.bind('click', function () {
        let m = {};
        m.mappingId = $(this).attr("mappingId");
        // 获取选中的值（兼容新的多选下拉框）
        m.sourceTable = $sourceSelect.val();
        m.targetTable = $targetSelect.val();
        if (undefined == m.sourceTable) {
            bootGrowl("请选择数据源表", "danger");
            return;
        }
        if (undefined == m.targetTable) {
            bootGrowl("请选择目标源表", "danger");
            return;
        }

        let sLen = m.sourceTable.length;
        let tLen = m.targetTable.length;
        if (sLen < 1) {
            bootGrowl("请选择数据源表", "danger");
            return;
        }
        if (tLen < 1) {
            bootGrowl("请选择目标源表", "danger");
            return;
        }
        if (sLen != tLen) {
            bootGrowl("表映射关系数量不一致，请检查源表和目标表关系", "danger");
            return;
        }

        m.sourceTable = m.sourceTable.join('|');
        m.targetTable = m.targetTable.join('|');
        m.sourceTablePK = $("#sourceTablePK").val();
        m.targetTablePK = $("#targetTablePK").val();

        doPoster("/tableGroup/add", m, function (data) {
            if (data.success == true) {
                bootGrowl("新增映射关系成功!", "success");
                refresh(m.mappingId);
            } else {
                bootGrowl(data.resultValue, "danger");
                if (data.status == 400) {
                    refresh(m.mappingId);
                }
            }
        });
    });
}

// 绑定删除表关系点击事件
function bindMappingTableGroupDelClick() {
    var $delBtn = $("#tableGroupDelBtn");
    
    // 检查按钮是否存在
    if (!$delBtn.length) {
        return;
    }
    
    $delBtn.unbind("click").click(function () {
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

// 绑定刷新表事件
function bindRefreshTablesClick() {
    let $refreshBtn = $("#refreshTableBtn");
    
    // 检查按钮是否存在
    if (!$refreshBtn.length) {
        return;
    }
    
    $refreshBtn.unbind("click").bind('click', function(){
        let id = $(this).attr("tableGroupId");
        doPoster("/mapping/refreshTables", {'id': id}, function (data) {
            if (data.success == true) {
                bootGrowl("刷新表成功!", "success");
                refresh(id);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
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
    // 绑定多值输入框事件
    if (typeof initMultipleInputTags === 'function') {
        initMultipleInputTags();
    }
    // 绑定删除表关系点击事件
    bindMappingTableGroupDelClick();
    //绑定刷新数据表按钮点击事件
    bindRefreshTablesClick();

    // 绑定下拉过滤按钮点击事件
    bindMultipleSelectFilterBtnClick();

    // 保存
    var $submitBtn = $("#mappingSubmitBtn");
    if ($submitBtn.length) {
        $submitBtn.click(function () {
            let $form = $("#mappingModifyForm");
            if ($form.length && $form.formValidate() == true) {
                let data = $form.serializeJson();
                submit(data);
            }
        });
    }

    // 返回
    var $backBtn = $("#mappingBackBtn");
    if ($backBtn.length) {
        $backBtn.click(function () {
            // 优先返回到同步任务管理页面，如果该函数不存在则返回到默认主页
            if (typeof loadMappingListPage === 'function') {
                // 返回到同步任务管理页面
                loadMappingListPage();
            } else if (typeof backIndexPage === 'function') {
                // 返回到默认主页
                backIndexPage();
            } else {
                // 最后的兜底方案：使用doLoader加载同步任务列表
                if (typeof doLoader === 'function') {
                    doLoader('/mapping/list', 0);
                } else {
                    // 如果都不存在，直接跳转
                    window.location.href = '/mapping/list';
                }
            }
        });
    }
})