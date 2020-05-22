// 查看详细数据
function bindQueryDataEvent() {
    $(".metaDataList .queryData").click(function () {
        var json = $(this).attr("json");
        var html = '<div class="row driver_break_word">' + json + '</div>';
        BootstrapDialog.show({
            title: "注意信息安全",
            type: BootstrapDialog.TYPE_INFO,
            message: html,
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "关闭",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });
}

// 清空数据
function bindClearEvent($btn, $title, $msg, $url){
    $btn.click(function () {
        var $id = $(this).attr("id");
        var data = {"id": $id};
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: $title,
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster($url, data, function (data) {
                        if (data.success == true) {
                            bootGrowl($msg, "success");
                            $initContainer.load('/monitor?id=' + $id);
                        } else {
                            bootGrowl(data.resultValue, "danger");
                        }
                    });
                    dialog.close();
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });

    });
}

$(function () {
    // 初始化select2插件
    $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });

    //连接器类型切换事件
    $("select[name='metaData']").change(function () {
        var $id = $(this).val();
        $initContainer.load('/monitor?id=' + $id);
    });

    bindQueryDataEvent();
    bindClearEvent($(".clearDataBtn"), "确认清空数据？", "清空数据成功!", "/monitor/clearData");
    bindClearEvent($(".clearLogBtn"), "确认清空日志？", "清空日志成功!", "/monitor/clearLog");

});