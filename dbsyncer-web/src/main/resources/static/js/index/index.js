// 添加分组
function bindAddProjectGroup() {
    $("#addProjectGroupBtn").click(function () {
        doLoader("/projectGroup/page/add");
    });
}

// 修改分组
function doEditProjectGroup($projectGroupSelect) {

    var $id = $projectGroupSelect;
    if (!isBlank($id)) {
        doLoader('/projectGroup/page/edit?id=' + $id);
        return;
    }
    bootGrowl("请选择分组", "danger");

}

// 删除分组
function doRemoveProjectGroup($projectGroupSelect) {

        var $id = $projectGroupSelect;
        if (isBlank($id)) {
            bootGrowl("请选择分组", "danger");
            return;
        }
        BootstrapDialog.show({
            title: "提示",
            type: BootstrapDialog.TYPE_INFO,
            message: "确认删除分组？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster('/projectGroup/remove', {id: $id}, function (data) {
                        if (data.success == true) {
                            // 显示主页
                            backIndexPage();
                            bootGrowl(data.resultValue, "success");
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
}

// 给分组下拉绑定事件
function bindProjectGroupSelect($projectGroupSelect) {
    $projectGroupSelect.off('change changed.bs.select');

    $projectGroupSelect.on('change', function () {
        $.loadingT(true);
        doLoader("/index?projectGroupId=" + $(this).val());
    });

}

// 添加连接
function bindAddConnector() {
    // 绑定添加连接按钮点击事件
    $("#indexAddConnectorBtn").click(function () {
        doLoader('/connector/page/add');
    });
}

// 编辑连接
function bindEditConnector() {
    $(".connectorList .dbsyncer_block").click(function () {
        var $id = $(this).attr("id");
        doLoader('/connector/page/edit?id=' + $id);
    });
}

// 添加驱动
function bindAddMapping() {
    $("#indexAddMappingBtn").click(function () {
        doLoader('/mapping/pageAdd');
    });
}

// 编辑驱动
function bindEditMapping() {
    $(".mappingList .dbsyncer_block").click(function () {
        var $id = $(this).attr("id");
        doLoader('/mapping/page/edit?id=' + $id);
    });
}

// 查看驱动日志
function bindQueryData() {
    $(".mappingList .queryData").click(function () {
        // 阻止触发click传递事件
        event.cancelBubble = true;
        var $menu = $('#menu > li');
        $menu.removeClass('active');
        $menu.find("a[url='/monitor']").parent().addClass('active');

        var $id = $(this).attr("id");
        doLoader('/monitor?dataStatus=0&id=' + $id);
    });
}

// 给连接下拉菜单绑定事件
function bindConnectorDropdownMenu() {
    // 绑定删除连接事件
    $(".connectorList .dropdown-menu li.remove").click(function () {
        var $url = $(this).attr("url");
        // 如果当前为恢复状态
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "确认删除连接？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPost($url);
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
    // 绑定复制连接事件
    $(".connectorList .dropdown-menu li.copy").click(function () {
        var $url = $(this).attr("url");
        doPost($url);
    });
}

// 给驱动下拉菜单绑定事件
function bindMappingDropdownMenu() {
    $(".mappingList .dropdown-menu li").click(function () {
        var $url = $(this).attr("url");
        var $confirm = $(this).attr("confirm");
        var $confirmMessage = $(this).attr("confirmMessage");

        if ("true" == $confirm) {
            // 如果当前为恢复状态
            BootstrapDialog.show({
                title: "警告",
                type: BootstrapDialog.TYPE_DANGER,
                message: $confirmMessage,
                size: BootstrapDialog.SIZE_NORMAL,
                buttons: [{
                    label: "确定",
                    action: function (dialog) {
                        doPost($url);
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

        doPost($url);
    });
}

function doPost(url) {
    doPoster(url, null, function (data) {
        if (data.success == true) {
            // 显示主页
            backIndexPage();
            bootGrowl(data.resultValue, "success");
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 创建定时器
function createTimer($projectGroupSelect) {
    doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
        if (data.success == true) {

            if (timer2 == null) {
                timer2 = setInterval(function () {
                    // 加载页面
                    //var projectGroupId = $projectGroupSelect.selectpicker('val');
                    var projectGroupId = $projectGroupSelect;
                    projectGroupId = (typeof projectGroupId === 'string') ? projectGroupId : '';
                    timerLoad("/index?projectGroupId=" + projectGroupId + "&refresh=" + new Date().getTime(), 1);
                }, data.resultValue * 1000);
            }

        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}


function refreshMappingList($projectGroupSelect) {

     doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
            if (data.success == true) {

                if (timer2 == null) {
                    timer2 = setInterval(function () {
                        // 加载页面
                        //var projectGroupId = $projectGroupSelect.selectpicker('val');
                        var projectGroupId = $projectGroupSelect;
                        projectGroupId = (typeof projectGroupId === 'string') ? projectGroupId : '';
                        $.ajax({
                               url: '/index/mappingdata?projectGroupId='+ projectGroupId + "&refresh=" + new Date().getTime(), // 假设这是获取驱动列表的接口
                               type: 'GET',
                               success: function(data) {

                                   // 重新绑定事件处理器（如果需要）
                                   var dataJson = JSON.parse(data);
                                   var datalist = dataJson.mappings;
                                   if(Array.isArray(datalist) ){
                                      // 遍历数组并拼接 div 字符串
                                        $.each(datalist, function(index, m) {
                                          var htmlContent = '';
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

                                          htmlContent += '<tbody>';
                                          htmlContent += '<tr>';
                                          htmlContent += '<td class="text-left">';
                                          htmlContent += modelname + '同步>总数:' + total;

                                          // 检查同步阶段是否为0（正在统计中）
                                          if (syncPhaseCode === 0) {
                                       	   if (counting) {
                                       		   htmlContent += '(正在统计中)';
                                       	   }
                                       	   if (total > 0 && (success + fail) > 0) {
                                       		   var progress = ((success + fail) / total * 100).toFixed(2);
                                       		   htmlContent += ',进度:' + progress + '%';
                                       	   }

                                       	   // 计算耗时
                                       	   var seconds = Math.floor((updateTime - beginTime) / 1000);
                                       	   htmlContent += ',耗时:';
                                       	   if (seconds < 60) {
                                       		   htmlContent += seconds + '秒';
                                       	   } else {
                                       		   var minutes = Math.floor(seconds / 60);
                                       		   htmlContent += minutes + '分' + (seconds - minutes * 60) + '秒';
                                       	   }
                                          }

                                          if (success > 0) {
                                       	   htmlContent += ',成功:' + success;
                                          }
                                          if (fail > 0) {
                                       	   htmlContent += ',失败:' + fail;
                                       	   htmlContent += ' <a id="' + id + '" href="javascript:;" class="label label-danger queryData">日志</a>';
                                          }
                                          htmlContent += '</td>';
                                          htmlContent += '</tr>';

                                          // 启动时间行
                                          htmlContent += '<tr>';
                                          htmlContent += '<td class="text-left">';
                                          htmlContent += '启动时间>';
                                          if (beginTime > 0) {
                                       	   var date = new Date(beginTime);
                                       	   htmlContent += date.getFullYear() + '-' +
                                       					 String(date.getMonth() + 1).padStart(2, '0') + '-' +
                                       					 String(date.getDate()).padStart(2, '0') + ' ' +
                                       					 String(date.getHours()).padStart(2, '0') + ':' +
                                       					 String(date.getMinutes()).padStart(2, '0') + ':' +
                                       					 String(date.getSeconds()).padStart(2, '0');
                                          }
                                          htmlContent += '</td>';
                                          htmlContent += '</tr>';
                                          htmlContent += '</tbody>';
                                          $("#"+mid).find(".table-hover").html(htmlContent);
                                       });
                                   }
                               },
                               error: function() {
                                   alert('刷新失败');
                               }
                           });
                    }, data.resultValue * 1000);
                }

            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });

}

function groupShow(id){
    var projectGroupId = (typeof id === 'string') ? id : '';
    timerLoad("/index?projectGroupId=" + projectGroupId + "&refresh=" + new Date().getTime(), 1);
    $("#projectGroup").val(projectGroupId);
}

$(function () {
    // 初始化select插件
    //initSelectIndex($(".select-control"));
    bindAddProjectGroup();
    var $projectGroupSelect = $("#projectGroup").val();

   // createTimer($projectGroupSelect);
   //异步刷新  同步进度 部分HTML
    refreshMappingList($projectGroupSelect);

    bindAddConnector();
    bindEditConnector();

    bindAddMapping();
    bindEditMapping();
    bindQueryData();

    bindConnectorDropdownMenu();
    bindMappingDropdownMenu();
});