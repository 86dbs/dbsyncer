$(function() {
    // 初始化页面
    initPage();
});

/**
 * 初始化页面
 */
function initPage() {
    // 绑定添加数据源按钮事件
    bindAddConnector();
      // 添加全局点击事件监听器，点击页面其他地方关闭下拉菜单
    $(document).click(function() {
        $('.dropdown-menu.show').removeClass('show');
    });
    
    // 绑定搜索相关事件
    bindConnectorSearch();
    
    // 加载数据源列表
    loadConnectorList();
}

/**
 * 绑定搜索相关事件
 */
function bindConnectorSearch() {
    // 搜索按钮点击事件
    $('#searchBtn').click(function() {
        performConnectorSearch();
    });

    // 搜索框回车事件
    $('#searchInput').keypress(function(e) {
        if (e.which === 13) { // Enter键
            performConnectorSearch();
        }
    });

    // 搜索类型下拉框change事件
    $('#searchType').change(function() {
        performConnectorSearch();
    });
}

/**
 * 执行数据源搜索
 */
function performConnectorSearch() {
    loadConnectorList();
}

/**
 * 加载数据源列表
 */
function loadConnectorList() {
    var searchType = $('#searchType').val();
    var keyword = $('#searchInput').val().trim();
    
    $.ajax({
        url: '/datasource/list',
        type: 'POST',
        data: {
            searchType: searchType,
            keyword: keyword
        },
        dataType: 'json',
        success: function(data) {
            if (data.success === true) {
                renderConnectorList(data.resultValue);
            } else {
                showError('加载数据源列表失败：' + data.resultValue);
            }
        },
        error: function() {
            showError('加载数据源列表失败');
        }
    });
}

/**
 * 渲染数据源列表
 * @param connectors 数据源列表数据
 */
function renderConnectorList(connectors) {
    var $connectorList = $('#connector-list');
    $connectorList.empty();
    
    if (!connectors || connectors.length === 0) {
        $connectorList.append('<div class="empty-message">暂无数据源</div>');
        return;
    }
    
    connectors.forEach(function(connector) {
        var connectorHtml = '';
        // 获取数据源类型，如果config中没有connectorType，则使用默认类型
        var dataSourceType = connector.config && connector.config.connectorType ? connector.config.connectorType : 'default';
        // 构建图片路径
        var imgPath = '/img/datasource/' + dataSourceType + '.png';
        connectorHtml += '<div class="connector-item" data-id="' + connector.id + '">';
        connectorHtml += '    <div class="connector-icon">';
        connectorHtml += '        <img src="' + imgPath + '" alt="' + dataSourceType + '" onerror="this.onerror=null;this.outerHTML=\'<i class=\\\"fa fa-database\\\"></i>\';" />';
        connectorHtml += '    </div>';
        connectorHtml += '    <div class="connector-info">';
        connectorHtml += '        <div class="connector-name">' + escapeHtml(connector.name) + '</div>';
//        connectorHtml += '        <div class="connector-type">' + escapeHtml(connector.type) + '</div>';
        connectorHtml += '    </div>';
        connectorHtml += '    <div class="connector-actions">';
        connectorHtml += '        <div class="dropdown">';
        connectorHtml += '            <button class="btn btn-default dropdown-toggle" type="button" data-toggle="dropdown">';
        connectorHtml += '                <i class="fa fa-ellipsis-v"></i>';
        connectorHtml += '            </button>';
        connectorHtml += '            <ul class="dropdown-menu">';
        connectorHtml += '                <li><a href="javascript:;" class="edit-connector">编辑</a></li>';
        connectorHtml += '                <li><a href="javascript:;" class="copy-connector">复制</a></li>';
        connectorHtml += '                <li><a href="javascript:;" class="delete-connector">删除</a></li>';
        connectorHtml += '            </ul>';
        connectorHtml += '        </div>';
        connectorHtml += '    </div>';
        connectorHtml += '</div>';
        
        var $connectorItem = $(connectorHtml);
        $connectorList.append($connectorItem);
        
        // 绑定数据源点击事件
        $connectorItem.click(function() {
            var connectorId = $(this).data('id');
            selectConnector(connectorId);
        });
        
        // 阻止下拉菜单点击事件冒泡
        $connectorItem.find('.connector-actions').click(function(e) {
            e.stopPropagation();
        });
        
        // 绑定下拉菜单显示/隐藏逻辑
        $connectorItem.find('.dropdown-toggle').click(function(e) {
            e.stopPropagation();
            // 先隐藏所有其他下拉菜单
            $('.dropdown-menu.show').removeClass('show');
            // 切换当前下拉菜单的显示状态
            $connectorItem.find('.dropdown-menu').toggleClass('show');
        });

        // 绑定编辑数据源事件
        $connectorItem.find('.edit-connector').click(function() {
            var connectorId = $connectorItem.data('id');
            bindEditConnector(connectorId);
            // 点击菜单项后隐藏下拉菜单
            $connectorItem.find('.dropdown-menu').removeClass('show');
        });
        
        // 绑定复制数据源事件
        $connectorItem.find('.copy-connector').click(function() {
            var connectorId = $connectorItem.data('id');
            copyConnector(connectorId);
            // 点击菜单项后隐藏下拉菜单
            $connectorItem.find('.dropdown-menu').removeClass('show');
        });
        
        // 绑定删除数据源事件
        $connectorItem.find('.delete-connector').click(function() {
            var connectorId = $connectorItem.data('id');
            deleteConnector(connectorId);
            // 点击菜单项后隐藏下拉菜单
            $connectorItem.find('.dropdown-menu').removeClass('show');
        });
    });
}

/**
 * 选择数据源，在右侧显示数据源详情
 * @param connectorId 数据源ID
 */
function selectConnector(connectorId) {
    // 移除所有数据源项的选中状态
    $('.connector-item').removeClass('selected');
    // 添加当前数据源项的选中状态
    $('.connector-item[data-id="' + connectorId + '"]').addClass('selected');
    
    // 加载数据源详情
    loadConnectorDetail(connectorId);
}

/**
 * 加载数据源详情
 * @param connectorId 数据源ID
 */
function loadConnectorDetail(connectorId) {
    $.ajax({
        url: '/datasource/connector/get?id=' + connectorId,
        type: 'GET',
        dataType: 'json',
        success: function(data) {
            if (data.success === true) {
                renderConnectorDetail(data.resultValue);
            } else {
               var errorMsg = typeof data.resultValue === 'string' ? data.resultValue : JSON.stringify(data.resultValue);
                showError('加载数据源详情失败：' + errorMsg);
            }
        },
        error: function() {
            showError('加载数据源详情失败');
        }
    });
}

/**
 * 渲染数据源详情
 * @param connector 数据源详情数据
 */
function renderConnectorDetail(connector) {
    var $connectorDetail = $('#connector-detail');
    $connectorDetail.empty();
    
    // 创建上下两栏布局结构
    var detailHtml = '';
    
    // 上半部分：数据源详情 (60%)
    //detailHtml += '<div class="detail-section">';
    detailHtml += '    <div class="connector-detail-header">';
    detailHtml += '        <h3>数据源详情</h3>';
    detailHtml += '    </div>';
    detailHtml += '    <div class="connector-detail-content">';
    
    // 数据源基本信息表单
    detailHtml += '    <div class="connector-details-info">';
    detailHtml += '        <input type="hidden" id="connector-id" value="' + connector.id + '">';
    
    detailHtml += '        <div class="detail-row">';
    detailHtml += '            <span class="detail-label">数据源名称</span>';
    detailHtml += '            <span class="detail-value">' + escapeHtml(connector.name) + '</span>';
    detailHtml += '        </div>';
    
    detailHtml += '        <div class="detail-row">';
    detailHtml += '            <span class="detail-label">数据源类型</span>';
    detailHtml += '            <span class="detail-value">' + escapeHtml(connector.type) + '</span>';
    detailHtml += '        </div>';
    
    // 渲染数据源配置参数
    if (connector.config && typeof connector.config === 'object') {
        for (var key in connector.config) {
            if (connector.config.hasOwnProperty(key)) {
                detailHtml += '        <div class="detail-row">';
                detailHtml += '            <span class="detail-label">' + getConfigLabel(key) + '</span>';
                detailHtml += '            <span class="detail-value">' + escapeHtml(connector.config[key]) + '</span>';
                detailHtml += '        </div>';
            }
        }
    }
    
    detailHtml += '    </div>';
    detailHtml += '    </div>';
    //detailHtml += '</div>';
    
    // 下半部分：关联驱动列表 (40%)
    // detailHtml += '<div class="drivers-section">';
    // detailHtml += '    <div class="drivers-header">';
    // detailHtml += '        <h4>关联驱动</h4>';
    // detailHtml += '        <span class="drivers-count">加载中...</span>';
    // detailHtml += '    </div>';
    // detailHtml += '    <div id="driver-list">';
    // detailHtml += '        <div class="no-drivers">正在加载关联驱动...</div>';
    // detailHtml += '    </div>';
    // detailHtml += '</div>';
    
    $connectorDetail.html(detailHtml);
    
    // 移除保存按钮事件绑定（因为按钮已移除）
    
    // 加载关联的驱动列表
    loadRelatedDrivers(connector.id);
}

/**
 * 加载与数据源关联的驱动列表
 * @param connectorId 数据源ID
 */
function loadRelatedDrivers(connectorId) {
    $.ajax({
        url: '/mapping/getRelatedMappings?connectorId=' + connectorId,
        type: 'GET',
        dataType: 'json',
        success: function(data) {
            if (data.success === true) {
                renderRelatedDrivers(data.resultValue);
            } else {
                var errorMsg = typeof data.resultValue === 'string' ? data.resultValue : JSON.stringify(data.resultValue);
                $('#drivers-list').html('<div class="no-drivers">加载关联任务失败：' + errorMsg + '</div>');
                $('.drivers-count').text('加载失败');
            }
        },
        error: function() {
            $('#drivers-list').html('<div class="no-drivers">加载关联任务失败</div>');
            $('.drivers-count').text('加载失败');
        }
    });
}

/**
 * 渲染关联驱动列表
 * @param drivers 驱动列表数据
 */
function renderRelatedDrivers(drivers) {
    var $driverList = $('#drivers-list');
    $driverList.empty();
    
    // 更新驱动数量
    $('.drivers-count').text('共 ' + (drivers ? drivers.length : 0) + ' 个任务');
    
    if (!drivers || drivers.length === 0) {
        $driverList.append('<div class="no-drivers">暂无关联任务</div>');
        return;
    }
    
    // 创建表格结构
    var tableHtml = '';
    tableHtml += '<div class="table-responsive">';
    tableHtml += '    <table class="table table-hover table-striped">';
    tableHtml += '        <thead>';
    tableHtml += '            <tr>';
    tableHtml += '                <th>名称</th>';
    tableHtml += '                <th>运行状态</th>';
    tableHtml += '                <th>起始源</th>';
    tableHtml += '                <th>目标源</th>';
    tableHtml += '                <th>同步类型</th>';
    tableHtml += '                <th>操作</th>';
    tableHtml += '                <th>创建时间</th>';
    tableHtml += '            </tr>';
    tableHtml += '        </thead>';
    tableHtml += '        <tbody>';
    
    drivers.forEach(function(driver) {
        // 处理同步类型显示
        var syncType = '';
        if (driver.model === 'full') {
            syncType = '全量';
        } else if (driver.model === 'increment') {
            syncType = '增量';
        } else if (driver.model === 'fullIncrement') {
            syncType = '混合';
        } else {
            syncType = '未知';
        }
        
        // 处理状态显示
        var stateHtml = '';
        var state = driver.meta ? driver.meta.state : 0;
        if (state === 0) {
            stateHtml = '<span class="label label-info">未运行</span>';
        } else if (state === 1) {
            stateHtml = '<span class="label label-success">运行中</span>';
        } else if (state === 2) {
            stateHtml = '<span class="label label-warning">停止中</span>';
        } else if (state === 3) {
            stateHtml = '<span class="label label-danger">异常</span>';
            if (driver.meta && driver.meta.errorMessage) {
                stateHtml += '<span title="' + escapeHtml(driver.meta.errorMessage) + '" class="mapping-error-sign" data-toggle="tooltip" data-placement="top"><i class="fa fa-exclamation-triangle"></i></span>';
            }
        }
        
        // 处理起始源显示
        var sourceHtml = '';
        if (driver.sourceConnector) {
            var sourceType = driver.sourceConnector.config && driver.sourceConnector.config.connectorType ? driver.sourceConnector.config.connectorType : 'default';
            var sourceImg = '/img/datasource/' + sourceType + '.png';
            sourceHtml = '<div style="display: flex; align-items: center;">';
            sourceHtml += '<img draggable="false" src="' + sourceImg + '" style="width: 20px; height: 20px; margin-right: 5px;" />';
            // 修复：移除数据源名称中的转义字符
            var sourceName = driver.sourceConnector.name ? driver.sourceConnector.name.replace(/["\\]/g, '') : '';
            sourceHtml += '<span>' + escapeHtml(sourceName) + '</span>';
            sourceHtml += '<span style="margin-left: 5px;">';
            sourceHtml += driver.sourceConnector.running ? '<i class="fa fa-circle well-sign-green"></i>' : '<i class="fa fa-times-circle-o well-sign-red"></i>';
            sourceHtml += '</span>';
            sourceHtml += '</div>';
        } else {
            sourceHtml = '-';
        }
        
        // 处理目标源显示
        var targetHtml = '';
        if (driver.targetConnector) {
            var targetType = driver.targetConnector.config && driver.targetConnector.config.connectorType ? driver.targetConnector.config.connectorType : 'default';
            var targetImg = '/img/datasource/' + targetType + '.png';
            targetHtml = '<div style="display: flex; align-items: center;">';
            targetHtml += '<img draggable="false" src="' + targetImg + '" style="width: 20px; height: 20px; margin-right: 5px;" />';
            // 修复：移除数据源名称中的转义字符
            var targetName = driver.targetConnector.name ? driver.targetConnector.name.replace(/["\\]/g, '') : '';
            targetHtml += '<span>' + escapeHtml(targetName) + '</span>';
            targetHtml += '<span style="margin-left: 5px;">';
            targetHtml += driver.targetConnector.running ? '<i class="fa fa-circle well-sign-green"></i>' : '<i class="fa fa-times-circle-o well-sign-red"></i>';
            targetHtml += '</span>';
            targetHtml += '</div>';
        } else {
            targetHtml = '-';
        }
        
        // 处理操作按钮
        var operationHtml = '';
        operationHtml += '<div class="operation-buttons">';
        // 只保留详情按钮，并使用内联样式精确控制大小
        operationHtml += '<a data-url="/mapping/page/edit?classOn=0&id=' + driver.id + '" href="javascript:;" class="btn btn-success" style="padding: 1px 6px; font-size: 10px; height: 20px; line-height: 14px;"><i class="fa fa-eye"></i> 详情</a>';
        operationHtml += '</div>';
        
        // 处理创建时间
        var timeHtml = '';
        if (driver.updateTime) {
            var date = new Date(driver.updateTime);
            timeHtml = date.getFullYear() + '-' +
                String(date.getMonth() + 1).padStart(2, '0') + '-' +
                String(date.getDate()).padStart(2, '0') + ' ' +
                String(date.getHours()).padStart(2, '0') + ':' +
                String(date.getMinutes()).padStart(2, '0') + ':' +
                String(date.getSeconds()).padStart(2, '0');
        } else {
            timeHtml = '-';
        }
        
        // 添加表格行
        tableHtml += '<tr id="' + driver.id + '">';
        tableHtml += '<td style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="' + escapeHtml(driver.name) + '">' + escapeHtml(driver.name) + '</td>';
        tableHtml += '<td>' + stateHtml + '</td>';
        tableHtml += '<td>' + sourceHtml + '</td>';
        tableHtml += '<td>' + targetHtml + '</td>';
        tableHtml += '<td>' + syncType + '</td>';
        tableHtml += '<td>' + operationHtml + '</td>';
        tableHtml += '<td>' + timeHtml + '</td>';
        tableHtml += '</tr>';
    });
    
    tableHtml += '        </tbody>';
    tableHtml += '    </table>';
    tableHtml += '</div>';
    
    $driverList.html(tableHtml);
    
    // 绑定操作按钮事件
    bindMappingOperationButtons();
}

/**
 * 查看驱动详情
 * @param driverItem 驱动项DOM元素
 */
function viewDriverDetail(driverItem) {
    var driverId = $(driverItem).data('id');
    // 跳转到驱动详情页面
    doLoader('/mapping/page/edit?classOn=0&id=' + driverId);
}

/**
 * 获取配置项的显示标签
 * @param key 配置项键名
 * @returns {string} 显示标签
 */
function getConfigLabel(key) {
    var labels = {
        'username': '用户名',
        'password': '密码',
        'url': '数据源URL',
        'host': '主机',
        'port': '端口',
        'database': '数据库名',
        'driverClassName': '驱动类名'
    };
    return labels[key] || key;
}

/**
 * 初始化可拖拽分割线
 */
function initResizer() {
    const resizer = document.getElementById('resizer');
    const detailSection = document.getElementById('connector-detail');
    const driversSection = document.getElementById('related-drivers');
    const rightPanel = document.querySelector('.right-panel');
    
    let isResizing = false;
    
    // 确保分割线始终可见
    if (resizer) {
        resizer.style.position = 'relative';
        resizer.style.zIndex = '10';
        resizer.style.height = '6px';
        resizer.style.backgroundColor = '#e0e0e0';
        resizer.style.cursor = 'ns-resize';
        
        // 鼠标按下事件
        resizer.addEventListener('mousedown', (e) => {
            e.preventDefault();
            isResizing = true;
            document.body.style.cursor = 'ns-resize';
            
            // 记录初始位置
            const startY = e.clientY;
            const startDetailHeight = detailSection.offsetHeight;
            const rightPanelHeight = rightPanel.offsetHeight;
            
            // 鼠标移动事件
            function handleMouseMove(e) {
                e.preventDefault();
                if (!isResizing) return;
                
                // 计算高度变化
                const deltaY = e.clientY - startY;
                const newDetailHeight = startDetailHeight + deltaY;
                
                // 计算百分比
                const detailPercent = (newDetailHeight / rightPanelHeight) * 100;
                
                // 限制最小高度为20%
                if (detailPercent >= 20 && detailPercent <= 80) {
                    detailSection.style.height = `${detailPercent}%`;
                    driversSection.style.height = `${100 - detailPercent}%`;
                }
            }
            
            // 鼠标释放事件
            function handleMouseUp() {
                isResizing = false;
                document.body.style.cursor = '';
                
                // 移除事件监听器
                document.removeEventListener('mousemove', handleMouseMove);
                document.removeEventListener('mouseup', handleMouseUp);
            }
            
            // 添加事件监听器
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
        });
        
        // 触摸设备支持
        resizer.addEventListener('touchstart', (e) => {
            e.preventDefault();
            isResizing = true;
            document.body.style.cursor = 'ns-resize';
            
            const startY = e.touches[0].clientY;
            const startDetailHeight = detailSection.offsetHeight;
            const rightPanelHeight = rightPanel.offsetHeight;
            
            function handleTouchMove(e) {
                e.preventDefault();
                if (!isResizing) return;
                
                const deltaY = e.touches[0].clientY - startY;
                const newDetailHeight = startDetailHeight + deltaY;
                const detailPercent = (newDetailHeight / rightPanelHeight) * 100;
                
                if (detailPercent >= 20 && detailPercent <= 80) {
                    detailSection.style.height = `${detailPercent}%`;
                    driversSection.style.height = `${100 - detailPercent}%`;
                }
            }
            
            function handleTouchEnd() {
                isResizing = false;
                document.body.style.cursor = '';
                
                document.removeEventListener('touchmove', handleTouchMove);
                document.removeEventListener('touchend', handleTouchEnd);
            }
            
            document.addEventListener('touchmove', handleTouchMove);
            document.addEventListener('touchend', handleTouchEnd);
        });
    }
}

// 页面加载完成后初始化
$(document).ready(function() {
    // 确保DOM元素已完全加载
    setTimeout(initResizer, 100);
});

/**
 * 保存数据源详情
 */
function saveConnectorDetail() {
    var connectorId = $('#connector-id').val();
    var name = $('#connector-name').val();
    
    if (!name) {
        showError('数据源名称不能为空');
        return;
    }
    
    // 收集配置信息
    var config = {};
    $('input[id^="config-"]').each(function() {
        var key = $(this).attr('id').replace('config-', '');
        config[key] = $(this).val();
    });
    // 构建表单数据
    var formData = new FormData();
    formData.append('id', connectorId);
    formData.append('name', name);
    // 将config转换为单独的参数
    for (var key in config) {
        if (config.hasOwnProperty(key)) {
            formData.append(key, config[key]);
        }
    }
    
    
    $.ajax({
        url: '/connector/edit',
        type: 'POST',
        data: formData,
        processData: false,
        contentType: false,
        success: function(result) {
            if (result.success === true) {
                showSuccess('保存成功');
                loadConnectorList(); // 重新加载数据源列表
                // 重新选中保存的数据源
                setTimeout(function() {
                    $('#connector-item-' + result.resultValue.id).click();
                }, 100);
            } else {
                showError('保存失败：' + (typeof result.resultValue === 'string' ? result.resultValue : JSON.stringify(result.resultValue)));
            }
        },
        error: function() {
            showError('保存失败');
        }
    });
}

/**
 * 绑定添加数据源按钮事件
 */
function bindAddConnector() {
    $('#add-connector').click(function() {
        doLoader('/connector/page/add');
    });
}

/**
 * 绑定编辑数据源事件
 * @param connectorId 数据源ID
 */
function bindEditConnector(connectorId) {
    doLoader('/connector/page/edit?id=' + connectorId);
}

/**
 * 复制数据源
 * @param connectorId 数据源ID
 */
function copyConnector(connectorId) {
    if (confirm('确定要复制该数据源吗？')) {
        $.ajax({
            url: '/connector/copy',
            type: 'POST',
            data: { id: connectorId },
            success: function(result) {
                if (result.success === true) {
                    showSuccess('复制成功');
                    loadConnectorList(); // 重新加载数据源列表
                } else {
                   showError('复制失败：' + (typeof result.resultValue === 'string' ? result.resultValue : JSON.stringify(result.resultValue)));
                }
            },
            error: function() {
                showError('复制失败');
            }
        });
    }
}

/**
 * 删除数据源
 * @param connectorId 数据源ID
 */
function deleteConnector(connectorId) {
    if (confirm('确定要删除该数据源吗？删除后将无法恢复。')) {
        $.ajax({
              url: '/connector/remove',
        type: 'POST',
        data: { id: connectorId },
        success: function(result) {
            if (result.success === true) {
                showSuccess('删除成功');
                loadConnectorList(); // 重新加载数据源列表
                // 清空右侧详情面板
                $('#connector-detail').html('<div class="no-connector-selected"><p>请从左侧选择一个数据源或点击"添加数据源"按钮</p></div>');
            } else {
                showError('删除失败：' + (typeof result.resultValue === 'string' ? result.resultValue : JSON.stringify(result.resultValue)));
            }
        },
        error: function() {
            showError('删除失败');
        }
    });
    }
}

/**
 * HTML转义
 * @param str 原始字符串
 * @returns {string} 转义后的字符串
 */
function escapeHtml(str) {
    if (!str) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
}

/**
 * 显示成功提示
 * @param message 提示信息
 */
function showSuccess(message) {
    alert(message || '操作成功');
}

/**
 * 显示错误提示
 * @param message 错误信息
 */
function showError(message) {
    alert(message || '操作失败');
}

/**
 * 给任务操作按钮绑定事件
 */
function bindMappingOperationButtons() {
    // 绑定所有操作按钮的点击事件
    $('.operation-buttons a').click(function(e) {
        e.preventDefault();
        e.stopPropagation();

        // 优先从data-url获取URL
        var url = $(this).data('url') || $(this).attr("href");
        // 移除javascript:;前缀
        url = url ? url.replace('javascript:;', '') : '';

        if (url) {
            // 详情按钮直接跳转到页面
            doLoader(url);
        }
    });
}

/**
 * 执行POST请求
 * @param url 请求URL
 */
function doPost(url) {
    $.ajax({
        url: url,
        type: 'GET',
        success: function(data) {
            if (data.success === true) {
                // 显示成功消息
                showSuccess(data.resultValue);
                // 重新加载当前数据源的关联任务
                var connectorId = $('#connector-id').val();
                if (connectorId) {
                    loadRelatedDrivers(connectorId);
                }
            } else {
                // 显示错误消息
                showError(data.resultValue);
            }
        },
        error: function() {
            showError('操作失败');
        }
    });
}