//切换连接器
function changeMetaData($this) {
    var $id = $this.val();
    $initContainer.load('/monitor?id=' + $id);
}

$(function () {
    // 初始化select2插件
    $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });

    //连接器类型切换事件
    $("select[name='metaData']").change(function () {
        changeMetaData($(this));
    });

    // 查看详细数据
    $(".metaDataList .queryData").click(function () {
        var json = $(this).attr("json");
        var html = '<div class="row driver_break_word">'+json+'</div>';
        BootstrapDialog.show({
            title: "注意信息安全",
            type: BootstrapDialog.TYPE_INFO,
            message: html,
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });

});