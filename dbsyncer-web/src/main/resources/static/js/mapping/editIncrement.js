
// 绑定增量策略切换事件
function bindMappingIncrementStrategyConfigChange(){
    var $mappingIncrementStrategyConfig = $("#mappingIncrementStrategyConfig");
    var $radio = $mappingIncrementStrategyConfig.find('input:radio[type="radio"]');
    // 初始化icheck插件
    $radio.iCheck({
        labelHover : false,
        cursor : true,
        radioClass : 'iradio_flat-blue',
    }).on('ifChecked', function(event) {
        showIncrementStrategyConfig($(this).val());
    });

    // 渲染选择radio配置
    var value = $mappingIncrementStrategyConfig.find('input[type="radio"]:checked').val();
    showIncrementStrategyConfig(value);
}

// 显示增量策略配置（日志/定时）
function showIncrementStrategyConfig($value){
    var $dqlConfig = $("#mappingIncrementStrategyDQLConfig");
    var $quartzConfig = $("#mappingIncrementStrategyQuartzConfig");
    if('log' == $value){
        $quartzConfig.addClass("hidden");
        $dqlConfig.removeClass("hidden");
    }else{
        $dqlConfig.addClass("hidden");
        $quartzConfig.removeClass("hidden");
    }
}

// 修改增量点配置
function bindMappingMetaSnapshotModifyClick(){
    $(".metaSnapshotModify").click(function(){
        var $value = $(this).parent().parent().find("td:eq(1)");
        var tmp = $value.text();
        $value.text("");
        $value.append("<input type='text'/>");
        var $input = $value.find("input");
        $input.focus().val(tmp);
        $input.blur(function(){
            $value.text($(this).val());
            if(tmp != $(this).val()){
                createMetaSnapshotParams();
            }
            $input.unbind();
        });
    })
}

// 生成增量点配置参数
function createMetaSnapshotParams(){
    var snapshot = {};
    $("#mappingMetaSnapshotConfig").find("tr").each(function(k,v){
        var key = $(this).find("td:eq(0)").text();
        var value = $(this).find("td:eq(1)").text();
        snapshot[key] = value;
    });
    $("#mappingMetaSnapshot").val(JSON.stringify(snapshot));
}

$(function() {
    // 绑定增量策略切换事件
    bindMappingIncrementStrategyConfigChange();
    // 绑定增量点配置修改事件
    bindMappingMetaSnapshotModifyClick();
});