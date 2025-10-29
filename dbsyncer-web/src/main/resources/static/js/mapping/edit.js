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
function refresh(id,classOn) {
    doLoader('/mapping/page/edit?id=' + id+"&classOn="+classOn);
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
    $tableGroupList.unbind("click");
    $tableGroupList.find("tr").bind('click', function () {
        doLoader('/tableGroup/page/editTableGroup?id=' + $(this).attr("id"));
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
    $(".bs-show-all").bind("click", function () {
        doLoader('/mapping/page/edit?id=' + $("#mappingId").val() + '&exclude=1');
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
                refresh(id,1);
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




function getMappping(id) {
     var intervalId = null; // 保存定时器ID
     doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
         if (data.success == true) {
           intervalId = setInterval(function () {
                     // 加载页面
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
                                   var success = meta.success || 0;
                                   var fail = meta.fail || 0;
                                   var beginTime = meta.beginTime || 0;
                                   var updateTime = meta.updateTime || 0;
                                   var syncPhase = meta.syncPhase || {};
                                   var syncPhaseCode = syncPhase.code || 0;
                                   var counting = meta.counting || false;
                                   var errorMessage = meta.errorMessage || '';
                                   var id = meta.id || '';
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
                                       if (total > 0 && (success + fail) > 0) {
                                          var progress = ((success + fail) / total * 100).toFixed(2);
                                          $("#metaPercent").find(".highlight-number").text(progress + "%");
                                          // 同步更新文本标签
                                          $("#progressText").text(progress + "%");
                                      } else {
                                          $("#metaPercent").find(".highlight-number").text("0%");
                                          $("#progressText").text("0%");
                                      }
                                  }
                                 // 重新绘制环形图
                                  updateProgressChart(total, success, fail);
                                    // 计算耗时
                     //               var seconds = Math.floor((updateTime - beginTime) / 1000);
                     //               htmlContent += ',耗时:';
                     //               if (seconds < 60) {
                     //                   htmlContent += seconds + '秒';
                     //               } else {
                     //                   var minutes = Math.floor(seconds / 60);
                     //                   htmlContent += minutes + '分' + (seconds - minutes * 60) + '秒';
                     //               }

                                   $("#metaModel").html('<p>总数:</p>' + '<span class="highlight-number total-color">'+total+'</span>');

                                   if (success > 0) {
                                     $("#metaSuccess").html('<p>成功:</p>' +'<span class="highlight-number success-color">'+ success+'</span>');
                                   }
                                   if (fail > 0) {
                                     $("#metaError").html('<p>失败:</p>' +'<span class="highlight-number error-color">'+ fail+'</span>');
                                   }

                                }
                            },
                            error: function() {
                               // alert('刷新失败');
                            }
                        });
                 }, data.resultValue * 1000);
         } else {
             bootGrowl(data.resultValue, "danger");
         }
     });
    // 页面卸载时清除定时器
    $(window).on('beforeunload', function () {
        if (intervalId !== null) {
            clearInterval(intervalId);
        }
    });

    // 返回时也清除定时器
    $("#mappingBackBtn").click(function () {
        if (intervalId !== null) {
            clearInterval(intervalId);
        }
    });

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
    //绑定刷新数据表按钮点击事件
    bindRefreshTablesClick();

    // 初始化select插件
    initSelectIndex($(".select-control-table"), -1);
    // 绑定下拉过滤按钮点击事件
    bindMultipleSelectFilterBtnClick();

    // 返回
    $("#mappingBackBtn").click(function () {
        backIndexPage();
    });

     // 创建环形图
    const $canvas = $('#progressChart');
    if ($canvas.length === 0) {
        console.error("Canvas element not found for progress chart");
        return;
    }

    const canvas = $canvas[0];
    const ctx = canvas.getContext('2d');

    // 获取数据
    const total = parseInt($('#metaModelHidden').val());
    const success = parseInt($('#successHidden').val());
    const fail = parseInt($('#failHidden').val());

    const progress = total > 0 ? (success + fail) / total : 0;
    const percentage = Math.round(progress * 100);

    // 更新文本
    $('#progressText').text(`${percentage}%`);

    // 创建图表
    const chart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [progress, 1 - progress],
                backgroundColor: [
                    '#3498db', // 蓝色填充
                    'rgba(255, 255, 255, 0.2)' // 白色背景
                ],
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

})