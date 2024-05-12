$(function () {
    const $pluginId = initSelect($("#pluginId"));
    // 绑定插件下拉选择事件
    const $pluginExtInfo = $("#pluginExtInfo");
    $pluginId.on('changed.bs.select',function(e){
        if(isBlank($(this).selectpicker('val'))){
            $pluginExtInfo.addClass("hidden");
        }else{
            $pluginExtInfo.removeClass("hidden");
        }
    });
    if(!isBlank($pluginId.selectpicker('val'))){
        $pluginExtInfo.removeClass("hidden");
    }
})