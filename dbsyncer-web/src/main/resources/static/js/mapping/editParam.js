// 绑定高级配置参数配置
function bingMappingParamsInputClick(){
    var $paramsList = $("#paramsList");
    var $inputs = $paramsList.find("input");
    $inputs.unbind();
    genMappingParams($inputs);
    $inputs.blur(function(){
        genMappingParams($inputs);
    });
}

// 生成参数配置
function genMappingParams($inputs){
    var params = {};
    $inputs.each(function () {
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