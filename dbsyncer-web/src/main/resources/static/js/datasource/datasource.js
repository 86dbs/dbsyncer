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
    detailHtml += '        <button id="save-connector-detail" class="btn btn-primary">保存</button>';
    detailHtml += '    </div>';
    detailHtml += '    <div class="connector-detail-content">';
    
    // 数据源基本信息表单
    detailHtml += '    <form id="connector-form">';
    detailHtml += '        <input type="hidden" id="connector-id" value="' + connector.id + '">';
    
    detailHtml += '        <div class="form-group">';
    detailHtml += '            <label for="connector-name">数据源名称</label>';
    detailHtml += '            <input type="text" id="connector-name" class="form-control" value="' + escapeHtml(connector.name) + '">';
    detailHtml += '        </div>';
    
    detailHtml += '        <div class="form-group">';
    detailHtml += '            <label for="connector-type">数据源类型</label>';
    detailHtml += '            <div id="connector-type" class="form-control" style="background-color: #f8f9fa; cursor: default;">' + escapeHtml(connector.type) + '</div>';
    detailHtml += '        </div>';
    
    // 渲染数据源配置参数
    if (connector.config && typeof connector.config === 'object') {
        for (var key in connector.config) {
            if (connector.config.hasOwnProperty(key)) {
                detailHtml += '        <div class="form-group">';
                detailHtml += '            <label for="config-' + key + '">' + getConfigLabel(key) + '</label>';
                detailHtml += '            <input type="text" id="config-' + key + '" class="form-control" value="' + escapeHtml(connector.config[key]) + '">';
                detailHtml += '        </div>';
            }
        }
    }
    
    detailHtml += '    </form>';
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
    
    // 绑定保存按钮事件
    $('#save-connector-detail').click(function() {
        saveConnectorDetail();
    });
    
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
                $('#drivers-list').html('<div class="no-drivers">加载关联驱动失败：' + errorMsg + '</div>');
                $('.drivers-count').text('加载失败');
            }
        },
        error: function() {
            $('#drivers-list').html('<div class="no-drivers">加载关联驱动失败</div>');
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
    $('.drivers-count').text('共 ' + (drivers ? drivers.length : 0) + ' 个驱动');
    
    if (!drivers || drivers.length === 0) {
        $driverList.append('<div class="no-drivers">暂无关联驱动</div>');
        return;
    }
    
    drivers.forEach(function(driver) {
        var driverHtml = '';
        driverHtml += '<div class="driver-item" data-id="' + driver.id + '" onclick="viewDriverDetail(this)">';
        driverHtml += '    <div class="driver-icon">';
        driverHtml += '        <i class="fa fa-exchange"></i>';
        driverHtml += '    </div>';
        driverHtml += '    <div class="driver-info">';
        driverHtml += '        <div class="driver-name">' + escapeHtml(driver.name) + '</div>';
        driverHtml += '        <div class="driver-status">';
        
        // 根据驱动类型显示不同状态
        if (driver.model === 'full') {
            driverHtml += '全量同步';
        } else if (driver.model === 'increment') {
            driverHtml += '增量同步';
        } else if (driver.model === 'fullIncrement') {
            driverHtml += '全量+增量同步';
        } else {
            driverHtml += '未知类型';
        }
        
        // 如果有状态信息，显示状态
        if (driver.status) {
            driverHtml += ' · ' + escapeHtml(driver.status);
        }
        
        driverHtml += '    </div>';
        driverHtml += '    </div>';
        driverHtml += '</div>';
        
        $driverList.append(driverHtml);
    });
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