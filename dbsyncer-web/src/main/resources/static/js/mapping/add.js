function submit(data) {
    doPoster("/mapping/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增驱动成功!", "success");
            // 保存成功后返回到同步任务管理页面，而不是编辑页面
            if (typeof loadMappingListPage === 'function') {
                // 返回到同步任务管理页面
                loadMappingListPage();
            } else if (typeof doLoader === 'function') {
                // 兜底方案：使用doLoader加载同步任务列表
                doLoader('/mapping/list', 0);
            } else {
                // 如果都不存在，直接跳转
                window.location.href = '/mapping/list';
            }
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 绑定开关切换事件（适配框架原生 form-switch 组件）
function bindToggleSwitch($switch, $toggle) {
    let $textarea = $toggle.find("textarea");
    
    // 监听原生 checkbox 的 change 事件
    $switch.on('change', function() {
        var isChecked = $(this).prop('checked');
        
        if (isChecked) {
            // 选中时：保存当前值，清空并隐藏 textarea
            $textarea.attr('tmp', $textarea.val());
            $textarea.val('');
            $toggle.addClass("hidden");
        } else {
            // 未选中时：恢复之前的值，显示 textarea
            $textarea.val($textarea.attr('tmp'));
            $textarea.removeAttr('tmp');
            $toggle.removeClass("hidden");
        }
    });
    
    // 初始化状态
    if ($switch.prop('checked')) {
        $textarea.attr('tmp', $textarea.val());
        $textarea.val('');
        $toggle.addClass("hidden");
    }
    
    return $switch;
}

// 存储连接器类型
var connectorTypes = {
    source: null,
    target: null
};

/**
 * 根据连接器类型动态显示数据库或Schema字段
 * 
 * @param {string} connectorId - 连接器ID
 * @param {string} type - 'source' 或 'target'
 */
function handleConnectorChange(connectorId, type) {
    console.log('[连接器变化] 连接器ID:', connectorId, '类型:', type);
    
    if (!connectorId) {
        connectorTypes[type] = null;
        
        // 清空数据库和Schema字段
        $("#" + type + "Database").html('<option value="">请先选择连接器</option>');
        $("#" + type + "Schema").val('');
        
        updateFieldsVisibility();
        return;
    }
    
    // 获取连接器信息
    $.ajax({
        url: '/mapping/getConnectorInfo',
        type: 'GET',
        data: { connectorId: connectorId },
        dataType: 'json',
        success: function(response) {
            if (response.success && response.resultValue) {
                var connector = response.resultValue;
                var connectorType = connector.config.connectorType.toLowerCase();
                
                console.log('[连接器变化] 连接器类型:', connectorType);
                
                // 保存连接器类型
                connectorTypes[type] = connectorType;
                
                // 无论什么类型的连接器，都尝试加载数据库列表
                loadDatabaseList(connectorId, type);
                
                // 更新字段显示状态
                updateFieldsVisibility();
            }
        },
        error: function(xhr, status, error) {
            console.error('[连接器变化] 获取连接器信息失败:', error);
            connectorTypes[type] = null;
            
            // 清空数据库和Schema字段
            $("#" + type + "Database").html('<option value="">获取连接器信息失败</option>');
            $("#" + type + "Schema").val('');
            
            updateFieldsVisibility();
        }
    });
}

/**
 * 根据源和目标连接器类型更新字段显示状态
 */
function updateFieldsVisibility() {
    var databaseGroup = $("#databaseGroup");
    var schemaGroup = $("#schemaGroup");
    var sourceDb = $("#sourceDatabase");
    var targetDb = $("#targetDatabase");
    var sourceSchema = $("#sourceSchema");
    var targetSchema = $("#targetSchema");
    
    var sourceType = connectorTypes.source;
    var targetType = connectorTypes.target;
    
    console.log('[字段可见性] 源类型:', sourceType, '目标类型:', targetType);
    
    // 判断是否需要显示数据库组和Schema组
    var showDatabaseGroup = false;
    var showSchemaGroup = false;
    
    // 源连接器：只要选中了连接器就显示数据库配置
    if (sourceType) {
        showDatabaseGroup = true;
        sourceDb.closest('.form-item').show();
    } else {
        sourceDb.closest('.form-item').hide();
        sourceDb.val('');
    }
    
    // 目标连接器：只要选中了连接器就显示数据库配置
    if (targetType) {
        showDatabaseGroup = true;
        targetDb.closest('.form-item').show();
    } else {
        targetDb.closest('.form-item').hide();
        targetDb.val('');
    }
    
    // 源连接器需要Schema配置（Oracle/SQL Server/PostgreSQL）
    if (sourceType && (sourceType.indexOf('oracle') !== -1 || 
                       sourceType.indexOf('sqlserver') !== -1 || 
                       sourceType.indexOf('postgresql') !== -1)) {
        showSchemaGroup = true;
        sourceSchema.closest('.form-item').show();
    } else {
        sourceSchema.closest('.form-item').hide();
        sourceSchema.val('');
    }
    
    // 目标连接器需要Schema配置（Oracle/SQL Server/PostgreSQL）
    if (targetType && (targetType.indexOf('oracle') !== -1 || 
                       targetType.indexOf('sqlserver') !== -1 || 
                       targetType.indexOf('postgresql') !== -1)) {
        showSchemaGroup = true;
        targetSchema.closest('.form-item').show();
    } else {
        targetSchema.closest('.form-item').hide();
        targetSchema.val('');
    }
    
    // 显示或隐藏整个组
    if (showDatabaseGroup) {
        databaseGroup.show();
        console.log('[字段可见性] 显示数据库配置组');
    } else {
        databaseGroup.hide();
        console.log('[字段可见性] 隐藏数据库配置组');
    }
    
    if (showSchemaGroup) {
        schemaGroup.show();
        console.log('[字段可见性] 显示Schema配置组');
    } else {
        schemaGroup.hide();
        console.log('[字段可见性] 隐藏Schema配置组');
    }
}

/**
 * 加载数据库列表
 * 
 * @param {string} connectorId - 连接器ID
 * @param {string} type - 'source' 或 'target'
 */
function loadDatabaseList(connectorId, type) {
    console.log('[加载数据库] 连接器ID:', connectorId, '类型:', type);
    
    var databaseSelect = $("#" + type + "Database");
    
    // 显示加载状态
    databaseSelect.html('<option value="">加载中...</option>');
    
    $.ajax({
        url: $basePath + '/connector/getDatabases',
        type: 'GET',
        data: { connectorId: connectorId },
        dataType: 'json',
        success: function(response) {
            console.log('[加载数据库] 响应结果:', response);
            
            if (response.success) {
                var databases = response.resultValue;
                
                // 清空并添加默认选项
                databaseSelect.html('<option value="">请选择数据库</option>');
                
                if (databases && databases.length > 0) {
                    // 添加数据库选项
                    databases.forEach(function(dbName) {
                        databaseSelect.append('<option value="' + dbName + '">' + dbName + '</option>');
                    });
                    console.log('[加载数据库] 成功加载 ' + databases.length + ' 个数据库');
                } else {
                    console.warn('[加载数据库] 数据库列表为空');
                    databaseSelect.html('<option value="">暂无数据库</option>');
                }
                
                // 增强 select 元素（如果 enhanceAllSelects 函数存在）
                if (typeof enhanceAllSelects === 'function') {
                    enhanceAllSelects();
                }
            } else {
                console.error('[加载数据库] 失败:', response.resultValue);
                databaseSelect.html('<option value="">加载失败</option>');
                bootGrowl("获取数据库列表失败: " + response.resultValue, "danger");
            }
        },
        error: function(xhr, status, error) {
            console.error('[加载数据库] 请求异常:', status, error);
            databaseSelect.html('<option value="">加载失败</option>');
            bootGrowl("获取数据库列表失败: " + error, "danger");
        }
    });
}

$(function () {
    // 分别初始化两个select插件，避免状态冲突
    var $sourceSelect = $("select[name='sourceConnectorId']");
    var $targetSelect = $("select[name='targetConnectorId']");

    // 监听数据源选择变化
    $sourceSelect.on('change', function() {
        var connectorId = $(this).val();
        handleConnectorChange(connectorId, 'source');
    });
    
    // 监听目标源选择变化
    $targetSelect.on('change', function() {
        var connectorId = $(this).val();
        handleConnectorChange(connectorId, 'target');
    });
    
    // 初始化时触发一次，处理默认选中的连接器
    if ($sourceSelect.val()) {
        handleConnectorChange($sourceSelect.val(), 'source');
    }
    if ($targetSelect.val()) {
        handleConnectorChange($targetSelect.val(), 'target');
    }

    // 绑定匹配相似表复选框事件
    bindToggleSwitch($('#autoMatchTable'), $("#tableGroups"));

    //保存
    $("#mappingSubmitBtn").click(function () {
        var $form = $("#mappingAddForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#mappingBackBtn").click(function () {
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
})