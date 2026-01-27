//*********************************** 驱动保存 开始位置***********************************//
function submit(data) {
    doPoster("/mapping/edit", data, function (data) {
        if (data.success == true) {
            bootGrowl("修改驱动成功!", "success");
            backIndexPage();
        } else {
            // 检查是否是目标表不存在的异常（通过错误码识别）
            if (data.status == 400 && data.resultValue &&
                typeof data.resultValue === 'object' &&
                data.resultValue.errorCode === 'TARGET_TABLE_NOT_EXISTS') {
                // 显示确认对话框
                showCreateTableConfirmDialogForMapping(data.resultValue);
            } else {
                // 其他错误，直接显示错误信息
                let errorMsg = typeof data.resultValue === 'string'
                    ? data.resultValue
                    : (data.resultValue && data.resultValue.message ? data.resultValue.message : '操作失败');
                bootGrowl(errorMsg, "danger");
            }
        }
    });
}
//*********************************** 驱动保存 结束位置***********************************//
// 刷新页面
function refresh(id,classOn) {
    updateHash('/mapping/page/edit?id=' + id+"&classOn="+classOn);
}

// 绑定修改驱动同步方式切换事件
function bindMappingModelChange() {
    // 尝试从隐藏字段获取同步类型
    var $modelInput = $('input[name="model"]');
    var $value = $modelInput.val();
    
    // 如果隐藏字段不存在或值为空，尝试从radio按钮获取
    if (!$value) {
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
                        refresh($("#mappingId").val(),0);
                    } else {
                        bootGrowl(response.resultValue, "danger");
                    }
                });
            }
        });

        // 渲染选择radio配置
        $value = $mappingModelChange.find('input[type="radio"]:checked').val();
    }
    
    // 显示驱动编辑配置（全量/增量）
    var $full = $("#mappingFullConfig");
    var $increment = $("#mappingIncrementConfig");
    if ('full' == $value) {
        $increment.addClass("hidden");
        $full.removeClass("hidden");
    } else if ('increment' == $value) {
        $full.addClass("hidden");
        $increment.removeClass("hidden");
    } else {
        // 对于混合同步模式，显示所有配置选项
        $full.removeClass("hidden");
        $increment.removeClass("hidden");
    }
}

// 绑定删除表关系复选框事件
function bindMappingTableGroupCheckBoxClick() {
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
function getCheckedBoxSize($checkbox) {
    var checked = [];
    $checkbox.each(function () {
        if ($(this).prop('checked')) {
            checked.push($(this).val());
        }
    });
    return checked;
}

// 绑定表关系点击事件
function bindMappingTableGroupListClick() {
    var $tableGroupList = $("#tableGroupList");
    if ($tableGroupList.length === 0) {
        // 如果tableGroupList不存在，延迟绑定
        setTimeout(function() {
            bindMappingTableGroupListClick();
        }, 100);
        return;
    }
    
    $tableGroupList.unbind("click");
    $tableGroupList.find("tr").bind('click', function (e) {
        // 如果点击的是编辑图标，不触发跳转
        if ($(e.target).hasClass('target-table-rename') || $(e.target).closest('.target-table-rename').length > 0) {
            return;
        }
        updateHash('/tableGroup/page/editTableGroup?id=' + $(this).attr("id"));
    });

    // 绑定表格拖拽事件
    $tableGroupList.tableDnD({
        onDrop: function (table, row) {
            var newData = [];
            var $trList = $(table).find("tr");
            $.each($trList, function () {
                newData.push($(this).attr('id'));
            });
            $("#sortedTableGroupIds").val(newData.join('|'));
        }
    });
    
    // 绑定目标表重命名事件
    bindTargetTableRenameClick();
}

// 绑定目标表重命名点击事件
function bindTargetTableRenameClick() {
    // 使用事件委托，支持动态添加的元素
    // 注意：需要绑定到tableGroupList容器上，而不是document，以确保事件能正确触发
    var $tableGroupList = $("#tableGroupList");
    if ($tableGroupList.length === 0) {
        // 如果tableGroupList不存在，使用document委托（作为备选方案）
        $(document).off('click', '.target-table-rename').on('click', '.target-table-rename', function(e) {
            e.stopPropagation();
            e.preventDefault();
            e.stopImmediatePropagation(); // 立即停止事件传播，防止其他处理器执行
            handleTargetTableRenameClick($(this));
            return false;
        });
    } else {
        $tableGroupList.off('click', '.target-table-rename').on('click', '.target-table-rename', function(e) {
            e.stopPropagation(); // 阻止事件冒泡，避免触发整行点击
            e.preventDefault(); // 阻止默认行为
            e.stopImmediatePropagation(); // 立即停止事件传播，防止其他处理器执行
            handleTargetTableRenameClick($(this));
            return false;
        });
    }
}

// 处理目标表重命名点击
function handleTargetTableRenameClick($icon) {
    var tableGroupId = $icon.data('id');
    var currentName = $icon.data('current-name');
    
    if (!tableGroupId) {
        bootGrowl("无法获取表映射ID", "danger");
        return false;
    }
    
    if (!currentName) {
        bootGrowl("无法获取当前表名", "danger");
        return false;
    }
    
    // 显示自定义模态框
    showTargetTableRenameModal(tableGroupId, currentName, $icon);
    return false;
}

// 显示目标表重命名模态框
function showTargetTableRenameModal(tableGroupId, currentName, $icon) {
    // 创建模态框HTML（如果不存在）
    if ($('#targetTableRenameModal').length === 0) {
        var modalHtml = '<div class="modal fade" id="targetTableRenameModal" tabindex="-1" role="dialog">' +
            '<div class="modal-dialog" role="document">' +
            '<div class="modal-content">' +
            '<div class="modal-header">' +
            '<button type="button" class="close" data-dismiss="modal" aria-label="Close">' +
            '<span aria-hidden="true">&times;</span>' +
            '</button>' +
            '<h4 class="modal-title">重命名目标表</h4>' +
            '</div>' +
            '<div class="modal-body">' +
            '<div class="form-group">' +
            '<label for="targetTableRenameInput">目标表名称 <span class="text-danger">*</span></label>' +
            '<input type="text" class="form-control" id="targetTableRenameInput" placeholder="请输入目标表名称" maxlength="128">' +
            '<small class="help-block text-muted">表名格式校验由目标数据库服务端处理</small>' +
            '</div>' +
            '</div>' +
            '<div class="modal-footer">' +
            '<button type="button" class="btn btn-default" data-dismiss="modal">取消</button>' +
            '<button type="button" class="btn btn-primary" id="targetTableRenameConfirmBtn">确定</button>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '</div>';
        $('body').append(modalHtml);
    }
    
    // 设置输入框的值
    $('#targetTableRenameInput').val(currentName);
    
    // 绑定确认按钮事件
    $('#targetTableRenameConfirmBtn').off('click').on('click', function() {
        var newName = $('#targetTableRenameInput').val().trim();
        if (!newName || newName === '') {
            bootGrowl("目标表名称不能为空", "danger");
            $('#targetTableRenameInput').focus();
            return;
        }
        if (newName === currentName) {
            $('#targetTableRenameModal').modal('hide');
            return;
        }
        
        // 关闭模态框
        $('#targetTableRenameModal').modal('hide');
        
        // 更新目标表名称
        updateTargetTableName(tableGroupId, newName, $icon);
    });
    
    // 回车键确认
    $('#targetTableRenameInput').off('keypress').on('keypress', function(e) {
        if (e.which === 13) { // Enter键
            $('#targetTableRenameConfirmBtn').click();
        }
    });
    
    // 显示模态框并聚焦输入框
    $('#targetTableRenameModal').modal('show');
    setTimeout(function() {
        $('#targetTableRenameInput').focus().select();
    }, 500);
}

// 更新目标表名称
function updateTargetTableName(tableGroupId, newTableName, $icon) {
    // 构建更新参数
    // 注意：不传 fieldMapping 参数，后端会自动映射全部源表字段
    var params = {
        'id': tableGroupId,
        'targetTable': newTableName
    };
    
    // 调用编辑API更新目标表名称
    doPoster("/tableGroup/edit", params, function (response) {
        if (response.success == true) {
            bootGrowl("目标表名称更新成功!", "success");
            // 只更新显示的名称，不刷新页面
            $icon.siblings('.target-table-name').text(newTableName);
            $icon.data('current-name', newTableName);
        } else {
            bootGrowl(response.resultValue || "更新失败", "danger");
        }
    });
}

// 绑定下拉选择事件自动匹配相似表事件
function bindTableSelect() {
    const $sourceSelect = $("#sourceTable");
    const $targetSelect = $("#targetTable");
    $sourceSelect.on('changed.bs.select', function (e) {
        $targetSelect.selectpicker('val', $(this).selectpicker('val'));
    });
    bindMappingTableGroupAddClick($sourceSelect, $targetSelect);
}

// 绑定下拉过滤按钮点击事件
function bindMultipleSelectFilterBtnClick() {
    $(".actions-btn").parent().append('<button type="button" class="actions-btn bs-show-all btn btn-default" title="显示所有表，包含已添加的表">取消过滤</button><button type="button" class="actions-btn bs-exclude-all btn btn-default" title="不显示已添加的表">过滤</button>');
    $(".actions-btn").css('width', '25%');
    $("#bs-show-all").on("click", function () {
        updateHash('/mapping/page/edit?id=' + $("#mappingId").val() + '&exclude=1');
        bootGrowl("取消过滤成功!", "success");
    });
    $(".bs-exclude-all").bind("click", function () {
        refresh($("#mappingId").val(),1);
        bootGrowl("过滤成功!", "success");
    });
}

// 绑定新增表关系点击事件
function bindMappingTableGroupAddClick($sourceSelect, $targetSelect) {
    let $addBtn = $("#tableGroupAddBtn");
    $addBtn.unbind("click");
    $addBtn.bind('click', function () {
        let m = {};
        m.mappingId = $(this).attr("mappingId");
        m.sourceTable = $sourceSelect.selectpicker('val');
        m.targetTable = $targetSelect.selectpicker('val');
        if (undefined == m.sourceTable) {
            bootGrowl("请选择数据源表", "danger");
            return;
        }
        // 如果未选择目标表，则使用源表作为目标表
        if (undefined == m.targetTable) {
            m.targetTable = m.sourceTable;
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
                // 直接重新加载页面内容，确保映射列表更新
                var url = '/mapping/page/edit?id=' + m.mappingId + "&classOn=1&refresh=" + new Date().getTime();
                doLoaderWithoutHashUpdate(url, 0);
            } else {
                bootGrowl(data.resultValue, "danger");
                if (data.status == 400) {
                    var url = '/mapping/page/edit?id=' + m.mappingId + "&classOn=1&refresh=" + new Date().getTime();
                    doLoaderWithoutHashUpdate(url, 0);
                }
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
            doPoster("/tableGroup/remove", { "mappingId": $mappingId, "ids": ids.join() }, function (data) {
                if (data.success == true) {
                    bootGrowl("删除映射关系成功!", "success");
                    // 直接重新加载页面内容，确保映射列表更新
                    var url = '/mapping/page/edit?id=' + $mappingId + "&classOn=1&refresh=" + new Date().getTime();
                    doLoaderWithoutHashUpdate(url, 0);
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });
}

// 修改驱动名称
function mappingModifyName() {
    var $name = $("#mappingModifyName");
    var tmp = $name.text();
    $name.text("");
    $name.append("<input type='text'/>");
    var $input = $name.find("input");
    $input.focus().val(tmp);
    $input.blur(function () {
        $name.text($(this).val());
        $("#mappingModifyForm input[name='name']").val($(this).val());
        $input.unbind();
    });
}

// 绑定刷新表事件
function bindRefreshTablesClick() {
    let $refreshBtn = $("#refreshTableBtn");
    $refreshBtn.bind('click', function () {
        let id = $(this).attr("tableGroupId");
        doPoster("/mapping/refreshTables", { 'id': id }, function (data) {
            if (data.success == true) {
                bootGrowl("刷新表成功!", "success");
                // 使用doLoader重新加载页面内容，确保下拉框显示最新数据
                doLoader('/mapping/page/edit?id=' + id + "&classOn=1");
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

function doMappingSubmit() {
    // 保存
    let $form = $("#mappingModifyForm");
    if ($form.formValidate() == true) {
        let data = $form.serializeJson();
        submit(data);
        //提交时刷新定时器
        clearTimer();
    }
}

/**
 * 显示创建表确认对话框（保存 mapping 场景）
 * @param errorInfo 错误信息对象，包含 errorCode, message, sourceTable, targetTable, missingTables 等
 */
function showCreateTableConfirmDialogForMapping(errorInfo) {
    let messageHtml = '<div style="padding: 10px;">';
    let missingTables = errorInfo.missingTables || [];
    let isMultiple = missingTables.length > 1;
    
    // 检查是否有多个缺失的表
    if (isMultiple) {
        // 多个表场景
        messageHtml += '<p><strong>以下 ' + missingTables.length + ' 个目标表不存在：</strong></p>';
        messageHtml += '<ul style="margin: 10px 0; padding-left: 20px; max-height: 300px; overflow-y: auto;">';
        for (let i = 0; i < missingTables.length; i++) {
            let mapping = missingTables[i];
            messageHtml += '<li>源表: <strong>' + mapping.sourceTable + '</strong> → 目标表: <strong>' + mapping.targetTable + '</strong></li>';
        }
        messageHtml += '</ul>';
        messageHtml += '<p>是否基于源表结构自动创建这些目标表？</p>';
    } else {
        // 单个表场景
        let mapping = missingTables[0] || null;
        if (mapping) {
            messageHtml += '<p><strong>目标表不存在：</strong>' + mapping.targetTable + '</p>';
            messageHtml += '<p style="color: #999; font-size: 12px;">源表：' + mapping.sourceTable + '</p>';
            messageHtml += '<p>是否基于源表结构自动创建目标表？</p>';
        }
    }
    
    messageHtml += '</div>';
    
    let dialog = BootstrapDialog.show({
        title: "目标表不存在",
        type: BootstrapDialog.TYPE_WARNING,
        message: messageHtml,
        size: BootstrapDialog.SIZE_NORMAL,
        buttons: [{
            label: "取消",
            cssClass: "btn-default",
            action: function (dialog) {
                dialog.close();
            }
        }, {
            label: isMultiple ? "批量创建表" : "创建表",
            cssClass: "btn-primary",
            action: function (dialog) {
                dialog.close();
                createTargetTablesAndRetry(errorInfo);
            }
        }]
    });
}

/**
 * 创建目标表并重试保存 mapping
 * @param errorInfo 错误信息对象，包含 missingTables
 */
function createTargetTablesAndRetry(errorInfo) {
    let missingTables = errorInfo.missingTables || [];
    if (missingTables.length === 0) {
        bootGrowl("没有需要创建的表", "warning");
        return;
    }
    
    let mappingId = $("#mappingId").val();
    if (!mappingId) {
        bootGrowl("无法获取映射ID", "danger");
        return;
    }
    
    // 显示加载提示
    let isMultiple = missingTables.length > 1;
    bootGrowl(isMultiple ? "正在批量创建目标表..." : "正在创建目标表...", "info");
    
    // 批量创建表（使用回调方式）
    let successCount = 0;
    let failCount = 0;
    let failMessages = [];
    let completedCount = 0;
    let totalCount = missingTables.length;
    
    // 如果没有表需要创建，直接返回
    if (totalCount === 0) {
        bootGrowl("没有需要创建的表", "warning");
        return;
    }
    
    // 逐个创建表
    function createNextTable(index) {
        if (index >= totalCount) {
            // 所有表创建完成，处理结果
            handleCreateResult();
            return;
        }
        
        let mapping = missingTables[index];
        let createParams = {
            mappingId: mappingId,
            sourceTable: mapping.sourceTable,
            targetTable: mapping.targetTable
        };
        
        doPoster("/tableGroup/createTargetTable", createParams, function (data) {
            completedCount++;
            if (data.success == true) {
                successCount++;
            } else {
                failCount++;
                let errorMsg = typeof data.resultValue === 'string' 
                    ? data.resultValue 
                    : (data.resultValue && data.resultValue.message ? data.resultValue.message : '创建表失败');
                failMessages.push(mapping.targetTable + ": " + errorMsg);
            }
            
            // 继续创建下一个表
            createNextTable(index + 1);
        });
    }
    
    // 处理创建结果
    function handleCreateResult() {
        if (failCount === 0) {
            bootGrowl("成功创建 " + successCount + " 个目标表，正在保存映射关系...", "success");
            // 所有表创建成功，重新提交保存 mapping
            setTimeout(function() {
                retrySubmitMapping();
            }, 500);
        } else {
            let message = "创建表完成：成功 " + successCount + " 个，失败 " + failCount + " 个";
            if (failMessages.length > 0) {
                message += "\n失败详情：\n" + failMessages.join("\n");
            }
            bootGrowl(message, failCount === totalCount ? "danger" : "warning");
            
            // 如果有部分成功，仍然尝试保存（后端会再次检查表是否存在）
            if (successCount > 0) {
                setTimeout(function() {
                    retrySubmitMapping();
                }, 1000);
            }
        }
    }
    
    // 开始创建第一个表
    createNextTable(0);
}

/**
 * 重试提交保存 mapping
 */
function retrySubmitMapping() {
    let $form = $("#mappingModifyForm");
    if ($form.length === 0) {
        bootGrowl("无法找到表单", "danger");
        return;
    }
    
    if ($form.formValidate() == true) {
        let data = $form.serializeJson();
        submit(data);
    } else {
        bootGrowl("表单验证失败", "danger");
    }
}

// updateProgressChart 改进版
function updateProgressChart(total, success, fail) {
    const $canvas = $('#progressChart');
    if ($canvas.length === 0) return;

    const canvas = $canvas[0];
    const ctx = canvas.getContext('2d');
    const progress = total > 0 ? (success + fail) / total : 0;

    let chart = Chart.getChart(canvas);
    if (chart) {
        chart.data.datasets[0].data[0] = progress;
        chart.update();
    } else {
        new Chart(ctx, {
            type: 'doughnut',
            data: {
                datasets: [{
                    data: [progress, 1 - progress],
                    backgroundColor: ['#3498db', 'rgba(255, 255, 255, 0.2)'],
                    borderColor: '#3498db',
                    borderWidth: 5,
                    cutout: '70%'
                }]
            },
            options: {
                responsive: false,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false }
                },
                animation: {
                    animateRotate: true,
                    animateScale: true
                }
            }
        });
    }
}


 var timer3 = null;
// 全局变量，用于保存当前任务ID
var currentMappingId = null;

function getMappping(id) {
    // 先清除旧定时器，确保每次调用都使用最新的id
    clearTimer();
    // 更新当前任务ID
    currentMappingId = id;

    // 立即加载一次数据，确保页面初始加载时就能显示表数据
    $.ajax({
        url: '/mapping/get?id='+ id + "&refresh=" + new Date().getTime(),
        type: 'GET',
        success: function(dataJson) {
            // 重新绑定事件处理器（如果需要）
            var success = dataJson.success;
            var m = dataJson.resultValue;
            if(success && m ){
                // 安全访问对象属性
                var mid = m && m.id ? m.id : '';
                var meta = m && m.meta ? m.meta : {};
                var metaId = meta.id || '';
                var total = meta.total || 0;
                var successCount = meta.success || 0;
                var failCount = meta.fail || 0;
                
                // 更新 metaId 隐藏字段
                $("#metaId").val(metaId);

                // 更新表映射关系数据
                updateTableGroups(mid, total, successCount, failCount);
            }
        },
        error: function() {
            // alert('刷新失败');
        }
    });

    doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
        if (data.success == true) {
            // 保存当前闭包中的任务ID
            var localId = id;
            // 检查当前任务ID是否与闭包中的任务ID相同，如果不同则不创建定时器
            if (currentMappingId === localId) {
                // 直接创建新定时器，因为已经清除了旧的
                timer3 = setInterval(function () {
                    // 再次检查当前任务ID是否与闭包中的任务ID相同
                    if (currentMappingId === localId) {
                        $.ajax({
                            url: '/mapping/get?id='+ localId + "&refresh=" + new Date().getTime(),
                            type: 'GET',
                            success: function(dataJson) {
                                // 重新绑定事件处理器（如果需要）
                                var success = dataJson.success;
                                var m = dataJson.resultValue;
                                if(success && m ){
                                    // 安全访问对象属性
                                    var mid = m && m.id ? m.id : '';
                                    var model = m && m.model ? m.model : '';
                                    var modelname =  '';
                                    if(model == 'full'){
                                        modelname= '全量';
                                    }else if(model == 'increment'){
                                        modelname= '增量';
                                    }else if(model == 'fullIncrement'){
                                        modelname= '混合';
                                    }
                                    var meta = m && m.meta ? m.meta : {};
                                    var total = meta.total || 0;
                                    var successCount = meta.success || 0;
                                    var failCount = meta.fail || 0;
                                    var beginTime = meta.beginTime || 0;
                                    var updateTime = meta.updateTime || 0;
                                    var syncPhase = meta.syncPhase || {};
                                    var syncPhaseCode = syncPhase.code || 0;
                                    var counting = meta.counting || false;
                                    var errorMessage = meta.errorMessage || '';
                                    var metaId = meta.id || '';
                                    var state = meta.state || '';
                                    
                                    // 更新 metaId 隐藏字段
                                    $("#metaId").val(metaId);

                                    var stateHtmlContent = '<p>任务状态：</p>';
                                    if(state == 0){
                                        stateHtmlContent += '<span class="highlight-number total-color">未运行</span>';
                                    }else if(state == 1){
                                        stateHtmlContent += '<span class="highlight-number success-color">运行中</span>';
                                    }else if(state == 2){
                                        stateHtmlContent += '<span class="highlight-number error-color">停止中</span>';
                                    }else if(state == 3){
                                        stateHtmlContent += '<span class="highlight-number error-color">异常</span>';
                                    }
                                    $("#mappingState").html(stateHtmlContent);

                                    $("#metaPercent").find("p").text("数据同步进度:");
                                    if (counting) {
                                        $("#metaPercent").find(".highlight-number").text("(正在统计中)");
                                    } else {
                                        if (total > 0 && (successCount + failCount) > 0) {
                                            var progress = ((successCount + failCount) / total * 100).toFixed(2);
                                            $("#metaPercent").find(".highlight-number").text(progress + "%");
                                            // 同步更新文本标签
                                            $("#progressText").text(progress + "%");
                                        } else {
                                            $("#metaPercent").find(".highlight-number").text("0%");
                                            $("#progressText").text("0%");
                                        }
                                    }
                                    // 重新绘制环形图
                                    updateProgressChart(total, successCount, failCount);

                                    $("#metaModel").html('<p>总数:</p>' + '<span class="highlight-number total-color">'+total+'</span>');

                                    if (successCount > 0) {
                                        $("#metaSuccess").html('<p>成功:</p>' +'<span class="highlight-number success-color">'+ successCount+'</span>');
                                    }
                                    if (failCount > 0) {
                                        $("#metaError").html('<p>失败:</p>' +'<span class="highlight-number error-color">'+ failCount+'</span>');
                                    }

                                    // 更新表映射关系数据
                                    updateTableGroups(mid, total, successCount, failCount);
                                }
                            },
                            error: function() {
                                // alert('刷新失败');
                            }
                        });
                    }
                }, data.resultValue * 1000);
            }
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
    //通过其他方式返回时也需要清除定时器
    bindNavbarEvents();
}
 // 绑定导航栏所有按钮的点击事件，清除timer3
function bindNavbarEvents() {
    // 1. 顶部菜单按钮（驱动、监控、插件、配置等）
    $("#menu a").click(function() {
        clearTimer();
    });

    // 2. 用户下拉菜单（修改个人信息、注销）
    $("#edit_personal, #nav_logout").click(function() {
        clearTimer();
    });

    // 3. 导航栏中可能存在的其他链接（如配置下拉菜单中的系统参数、用户管理）
    $("a[url]").click(function() {
       clearTimer();
    });
}

// 封装清除timer3的函数，避免重复代码
function clearTimer() {
    if (timer3 !== null) {
        clearInterval(timer3);
        timer3 = null;
    }
}

// 绑定编辑页面操作按钮事件
function bindEditPageOperationButtons() {
    $('.mapping-operation-buttons a').click(function (e) {
        e.preventDefault();
        e.stopPropagation();

        var $url = $(this).data('url') || $(this).attr("th:url") || $(this).attr("href");
        $url = $url ? $url.replace('javascript:;', '') : '';
        var $confirm = $(this).attr("confirm");
        var $confirmMessage = $(this).attr("confirmMessage");

        if ("true" == $confirm) {
            BootstrapDialog.show({
                title: "警告",
                type: BootstrapDialog.TYPE_DANGER,
                message: $confirmMessage,
                size: BootstrapDialog.SIZE_NORMAL,
                buttons: [{
                    label: "确定",
                    action: function (dialog) {
                        doPostForEditPage($url);
                        dialog.close();
                    }
                }, {
                    label: "取消",
                    action: function (dialog) {
                        dialog.close();
                    }
                }]
            });
            return;
        }

        doPostForEditPage($url);
    });
}

// 编辑页面专用的POST请求处理
function doPostForEditPage(url) {
    doPoster(url, null, function (data) {
        if (data.success == true) {
            var projectGroup = $("#projectGroup").val() || '';
            bootGrowl(data.resultValue, "success");
            
            if (url.indexOf('/mapping/remove') !== -1) {
                // 清除定时器，避免删除后仍尝试获取任务状态
                clearTimer();
                backIndexPage(projectGroup);
            } else {
                var mappingId = $("#mappingId").val();
                if (mappingId) {
                    refresh(mappingId, $("#mappingShow").val());
                }
            }
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 切换到指定的映射关系标签页
function nextToMapping(str) {
    // 获取映射关系标签页及对应链接
    const $baseConfigTab = $('#' + str);
    const $baseConfigLink = $('a[href="#' + str + '"]');

    if ($baseConfigTab.length && $baseConfigLink.length) {
        // 移除所有tab-pane的active类，再为目标标签页添加active
        $('.tab-pane').removeClass('active');
        $('.nav-tabs li').removeClass('active');
        $baseConfigTab.addClass('active');

        // 激活对应的tab链接及其父元素（通常是li）
        $baseConfigLink.addClass('active').parent().addClass('active');
    }
}

$(function () {

    var classOnVal =  $("#mappingShow").val();
    if(classOnVal && classOnVal==1){
        //从editTableGroup跳转回来后默认展示映射关系tab
        nextToMapping('mappingBaseConfig');
    }

    var mappingId =  $("#mappingId").val();
    if(mappingId){
        getMappping(mappingId);
    }

    // 绑定修改驱动同步方式切换事件
    bindMappingModelChange();

    // 绑定删除表映射事件
    bindMappingTableGroupCheckBoxClick();

    // 绑定表关系点击事件
    bindMappingTableGroupListClick();
    // 绑定下拉选择事件自动匹配相似表事件
    bindTableSelect();
    // 绑定多值输入框事件
    initMultipleInputTags();
    // 绑定删除表关系点击事件
    bindMappingTableGroupDelClick();
    //binding刷新数据表按钮点击事件
    bindRefreshTablesClick();

    // 初始化select插件
    initSelectIndex($(".select-control-table"), -1);
    // 绑定下拉过滤按钮点击事件
    bindMultipleSelectFilterBtnClick();

    // 绑定编辑页面操作按钮事件
    bindEditPageOperationButtons();

    // 返回
    $("#mappingBackBtn").click(function () {
         //清除定时器
         clearTimer();
         backIndexPage();
    });

    // 添加页面卸载事件监听，确保在用户离开页面时清除定时器
    $(window).on('beforeunload pagehide', function() {
        clearTimer();
        // 重置当前任务ID
        currentMappingId = null;
    });

})

// 更新表映射关系数据
function updateTableGroups(mappingId, total, successCount, failCount) {
    $.ajax({
        url: '/mapping/getTableGroups?id=' + mappingId,
        type: 'GET',
        success: function(dataJson) {
            var isSuccess = dataJson.success;
            var tableGroups = dataJson.resultValue;
            if (isSuccess && tableGroups) {
                var tbody = $("#tableGroupProgress tbody");
                tbody.empty();

                if (tableGroups.length > 0) {
                    $.each(tableGroups, function(index, tableGroup) {
                        var sourceTableName = tableGroup.sourceTable ? tableGroup.sourceTable.name : '';
                        var targetTableName = tableGroup.targetTable ? tableGroup.targetTable.name : '';

                        // 计算总同步数量（包括全量和增量）
                        var fullSuccess = tableGroup.fullSuccess || 0;
                        var incrementSuccess = tableGroup.incrementSuccess || 0;
                        var totalSyncCount = fullSuccess + incrementSuccess;

                        // 获取状态
                        var status = tableGroup.status || '正常';
                        var statusClass = status === '异常' ? 'label-danger' : 'label-success';
                        
                        // 获取同步速度
                        var currentSpeed = tableGroup.currentSpeed || 0;
                        var lastSyncTime = tableGroup.lastSyncTime || 0;
                        var currentTime = new Date().getTime();
                        // 如果最后同步时间超过5秒，则显示0条/秒
                        var speedDisplay = (currentTime - lastSyncTime > 5000) ? "0 条/秒" : (currentSpeed.toFixed(0) + " 条/秒");

                        // 计算同步状态
                        var syncStatus = "未开始";
                        var syncSortValue = 0; // 排序值
                        if (lastSyncTime > 0) {
                            var timeDiff = currentTime - lastSyncTime;
                            if (timeDiff < 5000) {
                                // 5秒内有同步记录，显示进行中
                                syncStatus = "进行中";
                                syncSortValue = 1;
                            } else {
                                // 计算分钟数
                                var minutes = Math.floor(timeDiff / (1000 * 60));
                                syncStatus = "已同步" + minutes + "分钟前的数据";
                                syncSortValue = 2 + (minutes / 10000); // 加上分钟数的小数值，确保时间顺序
                            }
                        }

                        var tr = '<tr>' +
                            '<td>' + sourceTableName + '</td>' +
                            '<td>' + targetTableName + '</td>' +
                            '<td data-sort="' + totalSyncCount + '">' + totalSyncCount + '</td>' +
                            '<td data-sort="' + syncSortValue + '">' + syncStatus + '</td>' +
                            '<td data-sort="' + currentSpeed + '">' + speedDisplay + '</td>' +
                            '<td><span class="label ' + statusClass + '">' + status + '</span></td>' +
                            '</tr>';
                        tbody.append(tr);
                    });
                } else {
                    var tr = '<tr><td colspan="6" class="text-center">暂无表映射关系</td></tr>';
                    tbody.append(tr);
                }
            }
            
            // 添加表格排序功能
            addTableSorting();
        },
        error: function() {
            // alert('获取表映射关系失败');
        }
    });
}

// 添加表格排序功能
function addTableSorting() {
    var table = $('#tableGroupProgress');
    var headers = table.find('thead th');
    
    // 为表头添加排序功能
    headers.each(function(index) {
        // 只为需要排序的列添加排序功能
        if (index === 2 || index === 3 || index === 4) { // 同步数量、同步状态、同步速度
            var header = $(this);
            header.css('cursor', 'pointer');
            header.addClass('sortable');
            
            // 添加排序图标
            if (!header.find('.sort-icon').length) {
                header.append('<span class="sort-icon" style="margin-left: 5px; font-size: 12px;">↕</span>');
            }
            
            // 绑定点击事件
            header.off('click').on('click', function() {
                var currentSort = header.data('sort') || 'asc';
                var newSort = currentSort === 'asc' ? 'desc' : 'asc';
                
                // 更新排序状态
                headers.each(function() {
                    $(this).data('sort', '');
                    $(this).find('.sort-icon').text('↕');
                });
                header.data('sort', newSort);
                header.find('.sort-icon').text(newSort === 'asc' ? '↑' : '↓');
                
                // 排序表格
                sortTable(table, index, newSort);
            });
        }
    });
}

// 排序表格
function sortTable(table, columnIndex, direction) {
    var tbody = table.find('tbody');
    var rows = tbody.find('tr').toArray();
    
    // 排序行
    rows.sort(function(a, b) {
        var aVal = $(a).find('td').eq(columnIndex).data('sort') || $(a).find('td').eq(columnIndex).text();
        var bVal = $(b).find('td').eq(columnIndex).data('sort') || $(b).find('td').eq(columnIndex).text();
        
        // 转换为数字进行比较
        if (!isNaN(aVal) && !isNaN(bVal)) {
            aVal = parseFloat(aVal);
            bVal = parseFloat(bVal);
        }
        
        if (aVal < bVal) {
            return direction === 'asc' ? -1 : 1;
        }
        if (aVal > bVal) {
            return direction === 'asc' ? 1 : -1;
        }
        return 0;
    });
    
    // 重新添加行
    tbody.empty();
    $.each(rows, function(index, row) {
        tbody.append(row);
    });
}

//*********************************** 错误队列 开始位置***********************************//
// 格式化日期
function formatErrorQueueDate(time) {
    var date = new Date(time);
    var YY = date.getFullYear() + '-';
    var MM = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
    var DD = (date.getDate() < 10 ? '0' + (date.getDate()) : date.getDate());
    var hh = (date.getHours() < 10 ? '0' + date.getHours() : date.getHours()) + ':';
    var mm = (date.getMinutes() < 10 ? '0' + date.getMinutes() : date.getMinutes()) + ':';
    var ss = (date.getSeconds() < 10 ? '0' + date.getSeconds() : date.getSeconds());
    return YY + MM + DD + " " + hh + mm + ss;
}

// 查看错误队列详细数据
function bindErrorQueueQueryDataDetailEvent() {
    let $queryData = $("#mappingErrorQueue .queryData");
    $queryData.unbind("click");
    $queryData.click(function () {
        let json = $(this).parent().find("div").text();
        BootstrapDialog.show({
            size: BootstrapDialog.SIZE_WIDE,
            title: "注意信息安全",
            type: BootstrapDialog.TYPE_INFO,
            message: function (dialog) {
                let $content = '<table class="table table-hover">';
                $content += '<thead><tr><th></th><th>字段</th><th>值</th></tr></thead>';
                $content += '<tbody id="dataDetail" tableGroupId="">';

                let jsonObj = $.parseJSON(json);
                let index = 1;
                $.each(jsonObj, function(name, value) {
                    $content += '<tr>';
                    $content += '<td>' + index + '</td>';
                    $content += '<td>' + name + '</td>';
                    $content += '<td class="driver_break_word">' + value + '</td>';
                    $content += '</tr>';
                    index++;
                });

                $content += '</tbody>';
                $content += '</table>';
                return $content;
            },
            buttons: [{
                label: "关闭",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });
}

// 错误队列重试同步
function bindErrorQueueQueryDataRetryEvent() {
    let $retry = $("#mappingErrorQueue .retryData");
    $retry.unbind("click");
    $retry.click(function () {
        let metaId = $("#metaId").val();
        let messageId = $(this).attr("id");
        updateHash('/monitor/page/retry?metaId=' + metaId + '&messageId=' + messageId);
    });
}

// 查看错误队列异常详情
function bindErrorQueueQueryErrorDetailEvent() {
    var $queryData = $("#mappingErrorQueue .queryError");
    $queryData.unbind("click");
    $queryData.click(function () {
        var json = $(this).text();
        var html = '<div class="row driver_break_word">' + json + '</div>';
        BootstrapDialog.show({
            title: "异常详细",
            size: BootstrapDialog.SIZE_WIDE,
            message: html,
            type: BootstrapDialog.TYPE_WARNING,
            buttons: [{
                label: "关闭",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });
}

// 错误队列显示更多
function errorQueueShowMore($this, $url, $params, $call) {
    $params.pageNum = parseInt($this.attr("num")) + 1;
    $params.pageSize = 10;
    doGetter($url, $params, function (data) {
        if (data.success == true) {
            if (data.resultValue.data.length > 0) {
                $this.attr("num", $params.pageNum);
            }
            $call(data.resultValue);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 错误队列显示数据
function errorQueueShowData($dataList, arr, append) {
    var html = '';
    var size = arr.length;
    if (size > 0) {
        var start = append ? $dataList.find("tr").size() : 0;
        for (i = 0; i < size; i++) {
            html += '<tr>';
            html += '<td>' + (start + i + 1) + '</td>';
            html += '<td>' + arr[i].targetTableName + '</td>';
            html += '<td>' + arr[i].event + '</td>';
            html += '<td>' + (arr[i].success ? '<span class="label label-success">成功</span>' : '<span class="label label-warning">失败</span>') + '</td>';
            html += '<td style="max-width:100px;" class="dbsyncer_over_hidden"><a href="javascript:;" class="dbsyncer_pointer queryError">' + arr[i].error + '</a></td>';
            html += '<td>' + formatErrorQueueDate(arr[i].createTime) + '</td>';
            html += '<td><div class="hidden">' + arr[i].json + '</div><a href="javascript:;" class="label label-info queryData">查看数据</a>&nbsp;';
            html += (arr[i].success ? '' : '<a id="' + arr[i].id + '" href="javascript:;" class="label label-warning retryData">重试</a>');
            html += '</td>';
            html += '</tr>';
        }
    }
    return html;
}

// 错误队列刷新数据列表
function errorQueueRefreshDataList(resultValue, append) {
    var $dataList = $("#errorQueueDataList");
    var $dataTotal = $("#errorQueueDataTotal");
    var html = errorQueueShowData($dataList, resultValue.data, append);
    if (append) {
        $dataList.append(html);
    } else {
        $dataList.html(html);
        $("#errorQueueQueryMore").attr("num", 1);
    }
    $dataTotal.html(resultValue.total);
    bindErrorQueueQueryDataDetailEvent();
    bindErrorQueueQueryDataRetryEvent();
    bindErrorQueueQueryErrorDetailEvent();
}

// 错误队列查询数据
function bindErrorQueueQueryDataEvent() {
    $("#errorQueueQueryBtn").click(function () {
        var keyword = $("#errorQueueKeyword").val();
        var id = $("#mappingId").val();
        var dataStatus = $("#errorQueueDataStatus").val();
        doGetter('/monitor/queryData', {
            "error": keyword,
            "dataStatus": dataStatus,
            "id": id,
            "pageNum": 1,
            "pageSize": 10
        }, function (data) {
            if (data.success == true) {
                errorQueueRefreshDataList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

// 错误队列显示更多
function bindErrorQueueQueryDataMoreEvent() {
    $("#errorQueueQueryMore").click(function () {
        var keyword = $("#errorQueueKeyword").val();
        var id = $("#mappingId").val();
        var dataStatus = $("#errorQueueDataStatus").val();
        errorQueueShowMore($(this), '/monitor/queryData', {
            "error": keyword,
            "dataStatus": dataStatus,
            "id": id
        }, function (resultValue) {
            errorQueueRefreshDataList(resultValue, true)
        });
    });
}

// 错误队列清空数据
function bindErrorQueueClearDataEvent() {
    var $clearDataBtn = $("#mappingErrorQueue .clearDataBtn");
    $clearDataBtn.click(function () {
        var $id = $("#mappingId").val();
        var data = {"id": $id};
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "确认清空该任务的错误数据?",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster('/monitor/clearData', data, function (data) {
                        if (data.success == true) {
                            bootGrowl("清空数据成功!", "success");
                            // 重新查询数据
                            $("#errorQueueQueryBtn").click();
                        } else {
                            bootGrowl(data.resultValue, "danger");
                        }
                    });
                    dialog.close();
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });
}

// 错误队列全部重做
function bindErrorQueueRetryAllEvent() {
    var $retryAllBtn = $("#errorQueueRetryAllBtn");
    $retryAllBtn.click(function () {
        var metaId = $("#metaId").val();
        if (!metaId) {
            bootGrowl("metaId为空，无法执行全部重做", "danger");
            return;
        }

        // 显示确认对话框，总数以后端处理为准
        BootstrapDialog.show({
            title: "确认全部重做",
            type: BootstrapDialog.TYPE_WARNING,
            message: "确认要重试所有失败数据吗？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                cssClass: "btn-primary",
                action: function (dialog) {
                    var $btn = $(this);
                    $btn.prop('disabled', true);
                    $btn.html('<i class="fa fa-spinner fa-spin"></i> 处理中...');

                    // 调用后端接口
                    doPoster('/monitor/retryAll', {"metaId": metaId}, function (result) {
                        $btn.prop('disabled', false);
                        $btn.html("确定");
                        dialog.close();

                        if (result.success == true) {
                            var retryResult = result.resultValue;
                            var message = "全部重做完成！总数: " + retryResult.total + 
                                          ", 成功: " + retryResult.success + 
                                          ", 失败: " + retryResult.fail;
                            bootGrowl(message, "success");
                            // 重新查询数据
                            $("#errorQueueQueryBtn").click();
                        } else {
                            bootGrowl(result.resultValue || "全部重做失败", "danger");
                        }
                    });
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });
}

// 初始化错误队列
function initErrorQueue() {
    // 绑定查询数据事件
    bindErrorQueueQueryDataEvent();
    // 绑定显示更多事件
    bindErrorQueueQueryDataMoreEvent();
    // 绑定清空数据事件
    bindErrorQueueClearDataEvent();
    // 绑定全部重做事件
    bindErrorQueueRetryAllEvent();
    // 绑定数据状态切换事件
    $("#errorQueueDataStatus").change(function () {
        $("#errorQueueQueryBtn").click();
    });
    // 默认查询数据
    $("#errorQueueQueryBtn").click();
}

// 监听标签页切换事件，当切换到错误队列标签时初始化数据
$(function() {
    $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
        var target = $(e.target).attr('href');
        if (target === '#mappingErrorQueue') {
            initErrorQueue();
        }
    });
});
//*********************************** 错误队列 结束位置***********************************//