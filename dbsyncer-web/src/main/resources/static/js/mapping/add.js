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

// 绑定开关切换事件
function bindToggleSwitch($switch, $toggle) {
    let $textarea = $toggle.find("textarea");
    return $switch.bootstrapSwitch({
        onText: "Yes",
        offText: "No",
        onColor: "success",
        offColor: "info",
        size: "normal",
        onSwitchChange: function (event, state) {
            if (state) {
                $textarea.attr('tmp', $textarea.val());
                $textarea.val('');
                $toggle.addClass("hidden");
            } else {
                $textarea.val($textarea.attr('tmp'));
                $textarea.removeAttr('tmp');
                $toggle.removeClass("hidden");
            }
        }
    });
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
    if (!connectorId) {
        connectorTypes[type] = null;
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
                
                // 保存连接器类型
                connectorTypes[type] = connectorType;
                
                // 根据连接器类型加载数据
                if (connectorType.indexOf('mysql') !== -1) {
                    loadDatabaseList(connectorId, type);
                } else {
                    // 清空对应的字段
                    $("#" + type + "Database").empty().append('<option value="">请选择数据库</option>');
                    $("#" + type + "Schema").val('');
                }
                
                // 更新字段显示状态
                updateFieldsVisibility();
            }
        },
        error: function(xhr, status, error) {
            console.error('获取连接器信息失败:', error);
            connectorTypes[type] = null;
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
    
    // 判断是否需要显示数据库组
    var showDatabaseGroup = false;
    var showSchemaGroup = false;
    
    // 源连接器需要数据库配置
    if (sourceType && sourceType.indexOf('mysql') !== -1) {
        showDatabaseGroup = true;
        sourceDb.closest('.col-sm-4').prev('label').show();
        sourceDb.closest('.col-sm-4').show();
    } else {
        sourceDb.closest('.col-sm-4').prev('label').hide();
        sourceDb.closest('.col-sm-4').hide();
        sourceDb.val('');
    }
    
    // 目标连接器需要数据库配置
    if (targetType && targetType.indexOf('mysql') !== -1) {
        showDatabaseGroup = true;
        targetDb.closest('.col-sm-4').prev('label').show();
        targetDb.closest('.col-sm-4').show();
    } else {
        targetDb.closest('.col-sm-4').prev('label').hide();
        targetDb.closest('.col-sm-4').hide();
        targetDb.val('');
    }
    
    // 源连接器需要Schema配置
    if (sourceType && (sourceType.indexOf('oracle') !== -1 || 
                       sourceType.indexOf('sqlserver') !== -1 || 
                       sourceType.indexOf('postgresql') !== -1)) {
        showSchemaGroup = true;
        sourceSchema.closest('.col-sm-4').prev('label').show();
        sourceSchema.closest('.col-sm-4').show();
    } else {
        sourceSchema.closest('.col-sm-4').prev('label').hide();
        sourceSchema.closest('.col-sm-4').hide();
        sourceSchema.val('');
    }
    
    // 目标连接器需要Schema配置
    if (targetType && (targetType.indexOf('oracle') !== -1 || 
                       targetType.indexOf('sqlserver') !== -1 || 
                       targetType.indexOf('postgresql') !== -1)) {
        showSchemaGroup = true;
        targetSchema.closest('.col-sm-4').prev('label').show();
        targetSchema.closest('.col-sm-4').show();
    } else {
        targetSchema.closest('.col-sm-4').prev('label').hide();
        targetSchema.closest('.col-sm-4').hide();
        targetSchema.val('');
    }
    
    // 显示或隐藏整个组
    if (showDatabaseGroup) {
        databaseGroup.show();
    } else {
        databaseGroup.hide();
    }
    
    if (showSchemaGroup) {
        schemaGroup.show();
    } else {
        schemaGroup.hide();
    }
}

/**
 * 加载数据库列表（用于MySQL）
 * 
 * @param {string} connectorId - 连接器ID
 * @param {string} type - 'source' 或 'target'
 */
function loadDatabaseList(connectorId, type) {
    var databaseSelect = $("#" + type + "Database");
    
    $.ajax({
        url: '/mapping/getDatabaseOrSchemaList',
        type: 'GET',
        data: { connectorId: connectorId },
        dataType: 'json',
        success: function(response) {
            if (response.success && response.resultValue) {
                var connector = response.resultValue;
                // 这里暂时使用连接器配置中的数据库信息
                // 后续可以扩展为从数据库实时获取数据库列表
                if (connector.config && connector.config.url) {
                    // 从URL中提取数据库名
                    var url = connector.config.url;
                    var match = url.match(/\/([^\/\?]+)(\?|$)/);
                    if (match && match[1]) {
                        databaseSelect.append('<option value="' + match[1] + '">' + match[1] + '</option>');
                        databaseSelect.val(match[1]);
                    }
                }
            }
        },
        error: function(xhr, status, error) {
            console.error('获取数据库列表失败:', error);
            databaseSelect.append('<option value="">获取数据库列表失败</option>');
        }
    });
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 分别初始化两个select插件，避免状态冲突
    var $sourceSelect = $("select[name='sourceConnectorId']");
    var $targetSelect = $("select[name='targetConnectorId']");
    
    initSelectIndex($sourceSelect, 0);
    initSelectIndex($targetSelect, 0);
   
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