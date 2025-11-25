// 修改驱动名称
function mappingModifyName() {
    const $name = $("#mapping_name_modify");
    const tmp = $name.text();
    $name.text("");
    $name.append("<input type='text'/>");
    const $input = $name.find("input");
    $input.focus().val(tmp);
    $input.blur(function() {
        $name.text($(this).val());
        $("#mappingModifyForm input[name='name']").val($(this).val());
        $input.unbind();
    });
}

// 绑定全量+增量切换事件
function bindMappingModelChange() {
    const mappingModel = $('#mapping_model').radioGroup({
        theme: 'info',
        size: 'md',
        onChange: function(value, label) {
            showSuperConfig(value);
        }
    }).data('radioGroup');
    // 渲染选择radio配置
    showSuperConfig(mappingModel.getValue());
}
function showSuperConfig(model){
    const full = $("#mapping_full");
    const increment = $("#mapping_increment");
    if ('full' === model) {
        increment.addClass("hidden");
        full.removeClass("hidden");
    } else {
        full.addClass("hidden");
        increment.removeClass("hidden");
    }
}

// 绑定日志+定时切换事件
function bindIncrementConfigChange(){
    const incrementStrategy = $('#increment_strategy').radioGroup({
        theme: 'info',
        size: 'md',
        onChange: function(value, label) {
            showIncrementConfig(value);
        }
    }).data('radioGroup');
    // 渲染选择radio配置
    showIncrementConfig(incrementStrategy.getValue());
}
function showIncrementConfig(model){
    const incrementQuartz = $("#increment_quartz");
    if ('log' === model) {
        incrementQuartz.addClass("hidden");
    } else {
        incrementQuartz.removeClass("hidden");
    }
}

// 修改增量点配置
function bindMappingMetaSnapshotModifyClick(){
    $(".metaSnapshotModify").click(function(){
        var $value = $(this).parent().parent().find("td:eq(1)");
        var tmp = $value.text();
        $value.text("");
        $value.append("<input type='text'/>");
        var $input = $value.find("input");
        $input.focus().val(tmp);
        $input.blur(function(){
            $value.text($(this).val());
            if(tmp !== $(this).val()){
                createMetaSnapshotParams();
            }
            $input.unbind();
        });
    })
}
// 生成增量点配置参数
function createMetaSnapshotParams(){
    var snapshot = {};
    $("#mappingMetaSnapshotConfig").find("tr").each(function(k,v){
        var key = $(this).find("td:eq(0)").text();
        var value = $(this).find("td:eq(1)").text();
        snapshot[key] = value;
    });
    $("#metaSnapshot").val(JSON.stringify(snapshot));
}

// 绑定删除表关系复选框事件
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

// 绑定表关系点击事件
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
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/mapping/list');
    };

    // 绑定全量+增量切换事件
    bindMappingModelChange();
    // 绑定日志+定时切换事件
    bindIncrementConfigChange();

    // 绑定表关系点击事件
    bindMappingTableGroupListClick();

    // 绑定删除表映射事件
    bindMappingTableGroupCheckBoxClick();
    // 绑定下拉选择事件自动匹配相似表事件
    // bindTableSelect();
    // 绑定多值输入框事件
    initMultipleInputTags();

    // 绑定删除表关系点击事件
    bindMappingTableGroupDelClick();
    //绑定刷新数据表按钮点击事件
    bindRefreshTablesClick();

    // 绑定下拉过滤按钮点击事件
    bindMultipleSelectFilterBtnClick();

    // 保存
    $("#mappingSubmitBtn").click(function () {
        let $form = $("#mappingModifyForm");
        if (DBSyncerTheme.validateForm($form)) {
            doPoster("/mapping/edit", $form.serializeJson(), function (response) {
                if (response.success === true) {
                    bootGrowl("修改驱动成功!", "success");
                    // 返回到默认主页
                    backIndexPage();
                } else {
                    bootGrowl(response.resultValue, "danger");
                }
            });
        }
    });

})