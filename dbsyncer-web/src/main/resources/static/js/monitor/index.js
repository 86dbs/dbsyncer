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

// 查看系统指标
function showSystemInfo(){
    $.getJSON("/app/health", function (data) {
        var details = data.details;
        var html = '';
        html += showPoint("硬盘", details.diskSpace);
        $("#systemList").html(html);
    });
}

// 内存
function showMem(){
    // 最大内存
    // jvm.memory.max
    // 已用内存
    // jvm.memory.used
    // 剩余内存
    // jvm.memory.committed
    $.getJSON("/app/metrics/jvm.memory.max", function (data) {
        var max = data.measurements[0].value;
        $.getJSON("/app/metrics/jvm.memory.used", function (data) {
            var used = data.measurements[0].value;
            $.getJSON("/app/metrics/jvm.memory.committed", function (data) {
                var committed = data.measurements[0].value;
                new Chart($("#cpuChart"),{
                    type: 'doughnut',//doughnut是甜甜圈,pie是饼状图
                    data: {
                        labels: ["最大%sGB", "已用", "空闲"],
                        datasets: [
                            {
                                backgroundColor: ["rgba(51,122,183, 0.8)","rgba(220,20,60, 0.8)","rgba(255,130,71,0.8)"],
                                data: [max, used, committed]
                            }
                        ]
                    },
                    options: {
                        title: {
                            display: true,
                            text: '内存使用情况'
                        },
                        legend: {
                            display: true,
                            position: 'bottom',
                            labels: {
                                fontColor: 'rgb(255, 99, 132)'
                            }
                        }
                    }
                });

            });
        });
    });

}

function showPoint(title, point){
    var status = point.status;
    var d = point.details;
    var total = (d.total / 1024 / 1024 / 1024).toFixed(2);
    var threshold = (d.threshold / 1024 / 1024 / 1024).toFixed(2);
    var free = (d.free / 1024 / 1024 / 1024).toFixed(2);

    // UP/DOWN success/danger
    var statusColor = status == 'UP' ? 'success' : 'danger';

    // more than 63%/78% waring/danger
    var precent = Math.round(threshold / total);
    var barColor = precent >= 63 ? 'waring' : 'success';
    barColor = precent >= 78 ? 'danger' : barColor;

    var html = "";
    html += "<tr>";
    html += "   <td>" + title + "</td>";
    html += "   <td><span class='label label-" + statusColor + "'>" + status + "</span></td>";
    html += "   <td>";
    html += "       <div class='progress' title='总共" + total + "GB,已用" + threshold + "GB,空闲" + free + "GB'>";
    html += "           <div class='progress-bar progress-bar-" + barColor + " progress-bar-striped active' style='width: " + precent + "%'>" + threshold + "GB</div>";
    html += "           <div class='progress-bar' style='width: " + (100 - precent) + "%'>" + free + "GB</div>";
    html += "       </div>";
    html += "   </td>";
    html += "</tr>";
    return html;
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

    showMem();
    showSystemInfo();


    // 系统CPU
    // system.cpu.usage
    // 程序CPU
    // process.cpu.usage

    // 守护线程数
    // jvm.threads.daemon
    // 活跃线程数
    // jvm.threads.live
    // 峰值线程数
    // jvm.threads.peak
    // GC 耗时
    // jvm.gc.pause

});