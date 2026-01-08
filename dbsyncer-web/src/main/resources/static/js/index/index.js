// 添加分组
function bindAddProjectGroup() {
    $("#addProjectGroupBtn").click(function () {
        updateHash("/projectGroup/page/add");
    });
}

// 修改分组
function doEditProjectGroup(projectGroupId) {

    var id = projectGroupId;
    if (!isBlank(id)) {
        updateHash('/projectGroup/page/edit?id=' + id);
        return;
    }
    bootGrowl("请选择分组", "danger");
}

// 删除分组
function doRemoveProjectGroup(projectGroupId) {

    var id = projectGroupId;
    if (isBlank(id)) {
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
                doPoster('/projectGroup/remove', { id: id }, function (data) {
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
        updateHash("/index?projectGroupId=" + $(this).val(), 1);
    });

}

// 添加连接
function bindAddConnector() {
    // 绑定添加连接按钮点击事件
    $("#indexAddConnectorBtn").click(function () {
        updateHash('/connector/page/add');
    });
}

// 编辑连接
function bindEditConnector() {
    $(".connectorList .dbsyncer_block").click(function () {
        var $id = $(this).attr("id");
        updateHash('/connector/page/edit?id=' + $id);
    });
}

// 添加任务
function bindAddMapping() {
    $("#indexAddMappingBtn").click(function () {
        updateHash('/mapping/pageAdd');
    });
}

// 编辑任务
function bindEditMapping() {
    $(".mappingList .dbsyncer_block").click(function (e) {
        // 在卡片视图下，禁用点击功能
        if ($('#cardView').is(':visible')) {
            // 阻止事件冒泡和默认行为
            e.preventDefault();
            e.stopPropagation();
            return false;
        }
        
        // 在列表视图下，保持原有功能
        var $id = $(this).attr("id");
        if ($id) {
            updateHash('/mapping/page/edit?classOn=0&id=' + $id);

             // 清除定时器，停止定时刷新任务列表
            if (window.timer2) {
                clearInterval(window.timer2);
                window.timer2 = null;
            }
        }
    });
}

// 查看任务日志
function bindQueryData() {
    $(".mappingList .queryData").click(function (event) {
        // 阻止触发click传递事件
        event.cancelBubble = true;
        var $menu = $('#menu > li');
        $menu.removeClass('active');
        $menu.find("a[url='/monitor']").parent().addClass('active');

        var $id = $(this).attr("id");
        updateHash('/monitor?dataStatus=0&id=' + $id, 2);
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

// 给任务操作按钮绑定事件
function bindMappingOperationButtons() {
    // 绑定所有操作按钮的点击事件
    $('.operation-buttons a').click(function (e) {
        e.preventDefault();
        e.stopPropagation();

        // 优先从data-url获取URL
        var $url = $(this).data('url') || $(this).attr("th\:url") || $(this).attr("href");
        // 移除javascript:;前缀
        $url = $url ? $url.replace('javascript:;', '') : '';
        var $confirm = $(this).attr("confirm");
        var $confirmMessage = $(this).attr("confirmMessage");

        if ("true" == $confirm) {
            // 如果需要确认操作
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

        // 对于编辑按钮，使用updateHash而不是doLoader
        if ($(this).hasClass('fa-pencil') || $(this).find('.fa-pencil').length > 0 || $(this).hasClass('fa-eye') || $(this).find('.fa-eye').length > 0) {
            updateHash($url);
        } else if ($(this).hasClass('queryData') || $(this).hasClass('fa-file-text-o') || $(this).find('.fa-file-text-o').length > 0) {
            // 日志按钮特殊处理
            var $id = $(this).attr("id");
            var $menu = $('#menu > li');
            $menu.removeClass('active');
            $menu.find("a[url='/monitor']").parent().addClass('active');
            updateHash('/monitor?dataStatus=0&id=' + $id, 2);
        } else {
            doPost($url);
        }
    });
}

function doPost(url) {
    doPoster(url, null, function (data) {
        if (data.success == true) {
            // 显示主页
            var projectGroup = $("#projectGroup").val() || '';
            backIndexPage(projectGroup);
            bootGrowl(data.resultValue, "success");
        } else {
            // 检查是否是停止任务操作并且错误信息是"驱动已停止"
            if (url.indexOf('/mapping/stop?id=') !== -1 && data.resultValue === '驱动已停止.') {
                // 即使失败，也需要更新UI状态，因为任务确实已经停止
                var projectGroup = $("#projectGroup").val() || '';
                backIndexPage(projectGroup);
                bootGrowl("任务已停止", "info");
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        }
    });
}

// URL参数解析函数 - 更健壮的实现
function getUrlParam(name) {
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    var regex = new RegExp('[\\#&]' + name + '=([^&#]*)');
    var results = regex.exec(window.location.href);
    return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
}

// 从URL参数中同步projectGroupId到隐藏字段
function syncProjectGroupIdFromUrl() {
    var projectGroupId = getUrlParam('projectGroupId');
    if (projectGroupId) {
        $('#projectGroup').val(projectGroupId);
    }
}

// 创建定时器
function createTimer(projectGroupId) {
    // 移除旧的定时器机制，避免与refreshMappingList函数的定时器冲突
    // 统一使用refreshMappingList函数的定时刷新机制
    return;
    /*
    doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
        if (data.success == true) {
            // 确保timer2是全局变量
            if (!window.timer2) {
                window.timer2 = setInterval(function () {
                    // 加载页面
                    var pgId = (typeof projectGroupId === 'string') ? projectGroupId : '';
                    timerLoad("/index?projectGroupId=" + pgId + "&refresh=" + new Date().getTime(), 1);
                }, data.resultValue * 1000);
            }

        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
    */
}


function refreshMappingList() {

    // 检查当前是否在任务列表界面
    if (!window.location.hash.startsWith('#/index?') && !window.location.hash.startsWith('#/index&')) {
        // 如果不在任务列表界面，不创建定时器
        return;
    }

    doGetWithoutLoading("/monitor/getRefreshIntervalSeconds", {}, function (data) {
        if (data.success == true) {
            // 确保timer2是全局变量
            if (!window.timer2) {
                window.timer2 = setInterval(function () {
                    // 再次检查当前是否在任务列表界面
                    if (window.location.hash.startsWith('#/index?') || window.location.hash.startsWith('#/index&')) {
                        var projectGroupId = $("#projectGroup").val();
                        // 确保projectGroupId是有效的
                        if (!projectGroupId || projectGroupId === undefined) {
                            projectGroupId = '';
                        }
                        
                        // 加载页面
                        $.ajax({
                        url: '/index/mappingdata?projectGroupId=' + projectGroupId + "&refresh=" + new Date().getTime(), // 假设这是获取任务列表的接口
                        type: 'GET',
                        success: function (data) {
                            if (data.success && data.status == 200) {
                                var datalist = data.resultValue;
                                if (Array.isArray(datalist)) {
                                    // 遍历数组并拼接 div 字符串
                                    $.each(datalist, function (index, m) {
                                        // 安全访问对象属性
                                        var mid = m && m.id ? m.id : '';
                                        var model = m && m.model ? m.model : '';
                                        var modelname = '';
                                        if (model == 'full') {
                                            modelname = '全量';
                                        } else if (model == 'increment') {
                                            modelname = '增量';
                                        } else if (model == 'fullIncrement') {
                                            modelname = '混合';
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
                                        var stateVal = meta.state != null && meta.state !== '' ? parseInt(meta.state) : 0;
                                        var stateHtmlContent = '';
                                        
                                        // 生成状态HTML
                                        if (stateVal == 0) {
                                            stateHtmlContent += '<span class="running-state label label-info">未运行</span>';
                                        } else if (stateVal == 1) {
                                            stateHtmlContent += '<span class="running-through-rate well-sign-green">✔</span>';
                                            stateHtmlContent += '<span class="running-state label label-success">运行中</span>';
                                        } else if (stateVal == 2) {
                                            stateHtmlContent += '<span class="running-state label label-warning">停止中</span>';
                                        } else if (stateVal == 3) {
                                            stateHtmlContent += '<span class="running-state label label-danger">异常</span>';
                                            stateHtmlContent += '<span title=" ' + errorMessage + ' " class="mapping-error-sign" data-toggle="tooltip" data-placement="top"><i class="fa fa-exclamation-triangle"></i></span>';
                                        }

                                        // 更新卡片视图
                                        var cardHtmlContent = '';
                                        cardHtmlContent += '<tbody>';
                                        cardHtmlContent += '<tr>';
                                        cardHtmlContent += '<td class="text-left" style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;  max-width: 0; width: 100%;">';
                                        cardHtmlContent += modelname + '同步>总数:' + total;

                                        // 检查同步阶段是否为0（正在统计中）
                                        if (syncPhaseCode === 0) {
                                            if (counting) {
                                                cardHtmlContent += '(正在统计中)';
                                            }
                                            if (total > 0 && (success + fail) > 0) {
                                                var progress = ((success + fail) / total * 100).toFixed(2);
                                                cardHtmlContent += ',进度:' + progress + '%';
                                            }
                                            // 计算耗时
                                            var seconds = Math.floor((updateTime - beginTime) / 1000);
                                            cardHtmlContent += ',耗时:';
                                            if (seconds < 60) {
                                                cardHtmlContent += seconds + '秒';
                                            } else {
                                                var minutes = Math.floor(seconds / 60);
                                                cardHtmlContent += minutes + '分' + (seconds - minutes * 60) + '秒';
                                            }
                                        }

                                        if (success > 0) {
                                            cardHtmlContent += ',成功:' + success;
                                        }
                                        if (fail > 0) {
                                            cardHtmlContent += ',失败:' + fail;
                                            cardHtmlContent += ' <a id="' + id + '" href="javascript:;" class="label label-danger queryData">日志</a>';
                                        }
                                        cardHtmlContent += '</td>';
                                        cardHtmlContent += '</tr>';

                                        // 启动时间行
                                        cardHtmlContent += '<tr>';
                                        cardHtmlContent += '<td class="text-left">';
                                        cardHtmlContent += '启动时间>';
                                        if (beginTime > 0) {
                                            var date = new Date(beginTime);
                                            cardHtmlContent += date.getFullYear() + '-' +
                                                String(date.getMonth() + 1).padStart(2, '0') + '-' +
                                                String(date.getDate()).padStart(2, '0') + ' ' +
                                                String(date.getHours()).padStart(2, '0') + ':' +
                                                String(date.getMinutes()).padStart(2, '0') + ':' +
                                                String(date.getSeconds()).padStart(2, '0');
                                        }
                                        cardHtmlContent += '</td>';
                                        cardHtmlContent += '</tr>';
                                        cardHtmlContent += '</tbody>';
                                        
                                        // 更新卡片视图
                                        var $cardElement = $("#" + mid);
                                        if ($cardElement.length > 0) {
                                            // 只更新可见的卡片内容
                                            if ($cardElement.is(':visible')) {
                                                $("#" + mid).find(".table-hover").html(cardHtmlContent);
                                                $("#" + mid).find("#stateId").html(stateHtmlContent);
                                            }
                                        }
                                        
                                        // 更新列表视图
                                        var $listElement = $("#listView").find("tr[id='" + mid + "']");
                                        if ($listElement.length > 0) {
                                            // 更新状态列
                                            var $statusColumn = $listElement.find('td:nth-child(2)');
                                            $statusColumn.html(stateHtmlContent);
                                            
                                            // 更新创建时间列
                                            var $createTimeColumn = $listElement.find('td:nth-child(7)');
                                            var createTimeHtml = '-';
                                            if (updateTime > 0) {
                                                var date = new Date(updateTime);
                                                createTimeHtml = date.getFullYear() + '-' +
                                                    String(date.getMonth() + 1).padStart(2, '0') + '-' +
                                                    String(date.getDate()).padStart(2, '0') + ' ' +
                                                    String(date.getHours()).padStart(2, '0') + ':' +
                                                    String(date.getMinutes()).padStart(2, '0') + ':' +
                                                    String(date.getSeconds()).padStart(2, '0');
                                            }
                                            $createTimeColumn.html(createTimeHtml);
                                        }
                                    });
                                }
                            } else {
                                bootGrowl(data.resultValue, "danger");
                            }
                        },
                        error: function () {
                                bootGrowl('刷新失败', "danger");
                            }
                        });
                    }
                }, data.resultValue * 1000);
            }

        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });

}

function groupShow(id) {
    var projectGroupId = (typeof id === 'string') ? id : '';
    
    // 使用updateHash更新URL哈希，而不是直接调用timerLoad
    updateHash("/index?projectGroupId=" + projectGroupId + "&refresh=" + new Date().getTime(), 1);
    $("#projectGroup").val(projectGroupId);
    
    // 清除并重新初始化定时刷新机制
    if (window.timer2) {
        clearInterval(window.timer2);
        window.timer2 = null;
    }
    // 调用refreshMappingList而不是createTimer，确保只使用一种刷新机制
    refreshMappingList();
}

function bindMappingSearch() {
    // 搜索按钮点击事件
    $("#mappingSearchBtn").click(function () {
        performMappingSearch();
    });

    // 搜索框回车事件
    $("#mappingSearchInput").keypress(function (e) {
        if (e.which === 13) { // Enter键
            performMappingSearch();
        }
    });
}

function performMappingSearch() {
    var keyword = $("#mappingSearchInput").val().trim();
    
    // 如果关键词为空，显示所有任务并返回
    if (keyword === '') {
        var isCardView = $('#cardView').is(':visible');
        if (isCardView) {
            // 卡片视图：显示所有卡片的外层容器
            $('#cardView .col-md-4, #cardView .col-sm-6').show();
        } else {
            // 列表视图：显示所有行
            $(".table.table-hover tbody tr").show();
        }
        $("#noSearchResults").hide();
        return;
    }
    
    var projectGroupId = $("#projectGroup").val() || '';
    var isCardView = $('#cardView').is(':visible');

    $.ajax({
        url: '/index/searchMapping',
        type: 'GET',
        data: {
            keyword: keyword,
            projectGroupId: projectGroupId
        },
        success: function (data) {
            if (data.success && data.status === 200) {
                var searchResults = data.resultValue;
                $("#noSearchResults").hide();

                if (searchResults.length === 0) {
                    if (isCardView) {
                        // 卡片视图：隐藏所有卡片的外层容器
                        $('#cardView .col-md-4, #cardView .col-sm-6').hide();
                    } else {
                        // 列表视图：隐藏所有行
                        $(".table.table-hover tbody tr").hide();
                    }
                    $("#noSearchResults").show();
                } else {
                    // 获取搜索结果的ID集合
                    var resultIds = searchResults.map(function(result) { 
                        return result && result.id ? result.id : null;
                    }).filter(function(id) { 
                        return id !== null;
                    });
                    
                    if (isCardView) {
                        // 卡片视图：先隐藏所有外层容器
                        $('#cardView .col-md-4, #cardView .col-sm-6').hide();
                        // 然后显示匹配ID的卡片的外层容器
                        resultIds.forEach(function(id) {
                            var matchedCard = $('#cardView #' + id);
                            if (matchedCard.length > 0) {
                                matchedCard.closest('.col-md-4, .col-sm-6').show();
                            }
                        });
                    } else {
                        // 列表视图：先隐藏所有行，再显示匹配的行
                        $(".table.table-hover tbody tr").hide();
                        resultIds.forEach(function(id) {
                            $("tr[id='" + id + "']").show();
                        });
                    }
                }
            } else {
                bootGrowl(data.resultValue || '搜索失败', "danger");
            }
        },
        error: function (xhr, status, error) {
            bootGrowl('搜索请求失败', "danger");
        }
    });
}

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

// 布局切换功能
function bindViewToggle() {
    // 列表视图按钮
    $('#listViewBtn').click(function() {
        if (!$(this).hasClass('active')) {
            $(this).addClass('active');
            $('#cardViewBtn').removeClass('active');
            $('#listView').show();
            $('#cardView').hide();
            // 保存当前视图状态到本地存储
            localStorage.setItem('dbsyncerViewMode', 'list');
        }
    });
    
    // 卡片视图按钮
    $('#cardViewBtn').click(function() {
        if (!$(this).hasClass('active')) {
            $('#listViewBtn').removeClass('active');
            $(this).addClass('active');
            $('#listView').hide();
            $('#cardView').show();
            // 保存当前视图状态到本地存储
            localStorage.setItem('dbsyncerViewMode', 'card');
        }
    });
    
    // 初始化视图状态
    var savedViewMode = localStorage.getItem('dbsyncerViewMode');
    if (savedViewMode === 'card') {
        $('#cardViewBtn').click();
    }
}

$(function () {
    // 初始化select插件
    //initSelectIndex($(".select-control"));
    bindAddProjectGroup();

    
    // 从URL参数中获取projectGroupId并同步到隐藏字段
    var projectGroupId = getUrlParam('projectGroupId');
    if (projectGroupId) {
        $('#projectGroup').val(projectGroupId);
    }
    
    //异步刷新  同步进度 部分HTML
    refreshMappingList();

    bindAddConnector();
    bindEditConnector();

    bindAddMapping();
    bindEditMapping();
    bindQueryData();

    bindConnectorDropdownMenu();
    //bindMappingDropdownMenu();
    // 替换下拉菜单事件为直接按钮事件
    bindMappingOperationButtons();

    // 绑定任务搜索功能
    bindMappingSearch();
    
    // 绑定布局切换功能
    bindViewToggle();
    
    // 清理定时器，防止内存泄漏
    $(window).on('beforeunload', function() {
        if (window.timer2) {
            clearInterval(window.timer2);
            window.timer2 = null;
        }
    });
});