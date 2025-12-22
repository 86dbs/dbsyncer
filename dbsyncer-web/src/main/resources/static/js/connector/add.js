function submit(data) {
    doPoster("/connector/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增连接成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 绑定连接器类型切换事件
function bindConnectorChangeEvent($select) {
    // 默认渲染连接页面
    $select.on('change',function(e){
        changeConnectorType($(this));
    });

    changeConnectorType($select);
}

function changeConnectorType($select){
    // 直接使用原生select的val()方法获取选中值，完全避免依赖selectpicker
    var connType = $select.val();
    //获取连接配置元素
    var $connectorConfig = $("#connectorConfig");
    //清空配置
    $connectorConfig.html("");

    //加载页面
   $connectorConfig.load($basePath + "/connector/page/add" + connType, function() {
       // 移除select-control类，避免其他脚本尝试初始化它为selectpicker
        $('.select-control').removeClass('select-control').addClass('form-control');
        
        // 处理内部页面可能的selectpicker初始化尝试
        // 重写initSelect和initSelectIndex函数，使其不执行任何操作
        // window.initSelect = function($select) { return $select; };
        // window.initSelectIndex = function($select, index) { 
        //     if (index >= 0) {
        //         $.each($select, function() {
        //             var $option = $(this).find("option")[index];
        //             if ($option) {
        //                 $(this).val($option.value);
        //             }
        //         });
        //     }
        //     return $select;
        // };
    });
}

$(function () {
    // 兼容IE PlaceHolder
   // $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    var $select = $("#connectorType");
    // 移除select-control类，避免其他脚本尝试初始化它为selectpicker
    $select.removeClass('select-control').addClass('form-control');
    
    // 设置默认选中第一个选项（索引为1，因为索引0通常是默认请选择选项）
    var $option = $select.find("option")[1];
    if ($option) {
        $select.val($option.value);
    }
    
    // 绑定事件
    bindConnectorChangeEvent($select);
    
    // 重写initSelect和initSelectIndex函数，防止其他地方调用时出错
    // window.initSelect = function($select) { return $select; };
    // window.initSelectIndex = function($select, index) { return $select; };
    //保存
    $("#connectorSubmitBtn").click(function () {
        var $form = $("#connectorAddForm");
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