function submit(data) {
    var $btn = $("#connectorSubmitBtn");
    
    // 防止重复提交：检查按钮是否已被禁用
    if ($btn.prop('disabled')) {
        return;
    }
    
    // 禁用按钮并显示加载状态
    var originalText = $btn.html();
    $btn.html('<i class="fa fa-spinner fa-spin"></i> 保存中...').prop('disabled', true);
    
    doPoster("/connector/add", data, function (response) {
        // 恢复按钮状态
        $btn.html(originalText).prop('disabled', false);
        
        if (response.success == true) {
            bootGrowl("新增连接成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(response.resultValue, "danger");
        }
    });
}

// 绑定连接器类型切换事件
function bindConnectorChangeEvent($select) {
    // 默认渲染连接页面
    $select.on('changed.bs.select',function(e){
        changeConnectorType($(this));
    });

    changeConnectorType($select);
}

function changeConnectorType($select){
    //连接类型
    var connType = $select.selectpicker('val');
    //获取连接配置元素
    var $connectorConfig = $("#connectorConfig");
    //清空配置
    $connectorConfig.html("");

    //加载页面
    $connectorConfig.load($basePath + "/connector/page/add" + connType);
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    var $select = $("#connectorType");
    initSelectIndex($select, 1);
    // 绑定连接器类型切换事件
    bindConnectorChangeEvent($select);

    // 先解绑事件，避免重复绑定
    $("#connectorSubmitBtn").off('click');
    $("#connectorBackBtn").off('click');

    //保存
    $("#connectorSubmitBtn").click(function () {
        var $form = $("#connectorAddForm");
        var $btn = $(this);
        
        // 防止重复提交
        if ($btn.prop('disabled')) {
            return;
        }
        
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#connectorBackBtn").click(function () {
        // 显示主页
        backIndexPage();
    });
})