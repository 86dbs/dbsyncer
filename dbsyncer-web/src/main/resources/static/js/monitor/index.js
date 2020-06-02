function formatDate(time) {
   var date = new Date(time);
  var YY = date.getFullYear() + '-';
  var MM = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
  var DD = (date.getDate() < 10 ? '0' + (date.getDate()) : date.getDate());
  var hh = (date.getHours() < 10 ? '0' + date.getHours() : date.getHours()) + ':';
  var mm = (date.getMinutes() < 10 ? '0' + date.getMinutes() : date.getMinutes()) + ':';
  var ss = (date.getSeconds() < 10 ? '0' + date.getSeconds() : date.getSeconds());

  return YY + MM + DD +" "+hh + mm + ss;
}

// 全局Ajax get
function doGetter(url, params, action) {
    $.loadingT(true);
    $.get(url, params, function (data) {
        $.loadingT(false);
        action(data);
    }).error(function (xhr, status, info) {
        $.loadingT(false);
        bootGrowl("访问异常，请刷新或重试.", "danger");
    });
}

// 查看详细数据
function bindQueryDataDetailEvent() {
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

// 查看数据
function bindQueryDataEvent() {
    $("#queryDataBtn").click(function () {
        var keyword = $("#searchDataKeyword").val();
        var id = $("select[name='metaData']").select2("val");
        doGetter('/monitor/queryData', {"error": keyword, "id" : id}, function (data) {
            if (data.success == true) {
                showDataList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

// 查看日志
function bindQueryLogEvent() {
    $("#queryLogBtn").click(function () {
        var keyword = $("#searchLogKeyword").val();
        doGetter('/monitor/queryLog', {"json": keyword}, function (data) {
            if (data.success == true) {
                showLogList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

function showDataList(arr){
    var size = arr.length;
    var html = '';
    for(i = 0; i < size; i++) {
        html += '<tr>';
        html += '<td>' + (i + 1) + '</td>';
        html += '<td>' + arr[i].event + '</td>';
        html += '<td>' + (arr[i].success ? '<span class="label label-success">成功</span>' : '<span class="label label-warning">失败</span>') + '</td>';
        html += '<td>' + arr[i].error + '</td>';
        html += '<td>' + formatDate(arr[i].createTime) + '</td>';
        html += '<td><a json=' + arr[i].json + ' href="javascript:;" class="label label-info queryData">查看数据</a></td>';
        html += '</tr>';
    }
    $("#dataList").html(html);
    bindQueryDataDetailEvent();
}

function showLogList(arr){
    var size = arr.length;
    var html = '';
    for(i = 0; i < size; i++) {
        html += '<tr>';
        html += '<td>' + (i + 1) + '</td>';
        html += '<td>' + arr[i].json + '</td>';
        html += '<td>' + formatDate(arr[i].createTime) + '</td>';
        html += '</tr>';
    }
    $("#logList").html(html);
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

    bindQueryLogEvent();
    bindQueryDataEvent();
    bindQueryDataDetailEvent();
    bindClearEvent($(".clearDataBtn"), "确认清空数据？", "清空数据成功!", "/monitor/clearData");
    bindClearEvent($(".clearLogBtn"), "确认清空日志？", "清空日志成功!", "/monitor/clearLog");

});