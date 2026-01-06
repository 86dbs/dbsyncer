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
    var $value = $mappingModelChange.find('input[type="radio"]:checked').val();
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
                refresh(m.mappingId,1);
            } else {
                bootGrowl(data.resultValue, "danger");
                if (data.status == 400) {
                    refresh(m.mappingId,1);
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
                    refresh($mappingId,1);
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
                var total = meta.total || 0;
                var successCount = meta.success || 0;
                var failCount = meta.fail || 0;

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
        timer3 = null; // 重置全局定时器变量
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

    // 绑定同步方式切换事件
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

                        // 计算进度（使用每个表组自己的同步进度数据）
                        var tableTotal = tableGroup.estimatedTotal || tableGroup.total || 0;
                        var tableSuccess = tableGroup.success || 0;
                        var tableFail = tableGroup.fail || 0;
                        var progress = tableTotal > 0 ? ((tableSuccess + tableFail) / tableTotal * 100).toFixed(2) : 0;
                        var progressValue = parseFloat(progress);

                        // 创建与图二一致的进度条HTML结构
                        var progressHtml = '<div style="display: flex; align-items: center; width: 100%;">' +
                            '<div style="font-weight: bold; margin-right: 8px; min-width: 40px;">' + progress + '%</div>' +
                            '<div style="flex: 1; height: 10px; background-color: #f0f0f0; border-radius: 5px; overflow: hidden;">' +
                            '<div style="height: 100%; width: ' + progress + '%; background-color: #4CAF50; border-radius: 5px;"></div>' +
                            '</div>' +
                            '</div>';

                        var tr = '<tr>' +
                            '<td>' + sourceTableName + '</td>' +
                            '<td>' + targetTableName + '</td>' +
                            '<td>' + progressHtml + '</td>' +
                            '</tr>';
                        tbody.append(tr);
                    });
                } else {
                    var tr = '<tr><td colspan="3" class="text-center">暂无表映射关系</td></tr>';
                    tbody.append(tr);
                }
            }
        },
        error: function() {
            // alert('获取表映射关系失败');
        }
    });
}