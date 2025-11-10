$(function () {
    // 检查元素是否存在
    var $pluginId = $("#pluginId");
    if (!$pluginId.length) {
        // 如果元素不存在，直接返回，不执行后续代码
        return;
    }
    
    // 增强 select 元素（替代 Bootstrap selectpicker）
    if (typeof enhanceSelect === 'function') {
        $pluginId = enhanceSelect($pluginId);
    }
    
    // 获取插件扩展信息区域
    var $pluginExtInfo = $("#pluginExtInfo");
    if (!$pluginExtInfo.length) {
        console.warn('[editPlugin] 插件扩展信息元素不存在');
        return;
    }

    /**
     * 更新插件扩展信息显示状态
     */
    function updatePluginExtInfo() {
        var val = $pluginId.val();
        var isEmpty = false;
        
        // 检查值是否为空
        if (typeof isBlank === 'function') {
            isEmpty = isBlank(val);
        } else {
            isEmpty = (!val || val === '' || val === null);
        }
        
        // 根据值显示或隐藏扩展信息
        if (isEmpty) {
            $pluginExtInfo.addClass("hidden");
        } else {
            $pluginExtInfo.removeClass("hidden");
        }
    }

    // 绑定 change 事件（替代 Bootstrap 的 changed.bs.select）
    $pluginId.on('change', function(e) {
        updatePluginExtInfo();
    });
    
    // 也监听自定义的 select:changed 事件（兼容性）
    $pluginId.on('select:changed', function(e) {
        updatePluginExtInfo();
    });
    
    // 初始化时更新显示状态
    updatePluginExtInfo();
})