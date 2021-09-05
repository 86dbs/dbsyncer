// 绑定高级配置参数配置
function bingMappingParamsInputClick(){
    var $paramsList = $("#paramsList");
    var $select = $paramsList.find("select");
    genMappingParams($select);
    // 下拉切换事件
    $select.change(function () {
        genMappingParams($select);
    });
}

// 生成参数配置
function genMappingParams($select){
    var params = {};
    $select.each(function () {
        if('' != $(this).val()){
            params[$(this).attr("name")] = $(this).val();
        }
    })
    $("#params").val(JSON.stringify(params));
}

$(function() {
    // 绑定高级配置参数配置
    bingMappingParamsInputClick();
});