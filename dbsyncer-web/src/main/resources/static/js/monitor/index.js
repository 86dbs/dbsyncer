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

// 查看详细数据
function bindQueryDataDetailEvent() {
    var $queryData = $(".queryData");
    $queryData.unbind("click");
    $queryData.click(function () {
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
                            doLoader('/monitor?id=' + $id);
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

// 显示更多
function showMore($this, $url, $params, $call){
    $params.pageNum = parseInt($this.attr("num")) + 1;
    $params.pageSize = 10;
    doGetter($url, $params, function (data) {
        if (data.success == true) {
            if(data.resultValue.data.length > 0){
                $this.attr("num", $params.pageNum);
            }
            $call(data.resultValue);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 查看数据
function bindQueryDataEvent() {
    $("#queryDataBtn").click(function () {
        var keyword = $("#searchDataKeyword").val();
        var id = $("select[name='metaData']").select2("val");
        doGetter('/monitor/queryData', {"error": keyword, "id" : id, "pageNum" : 1, "pageSize" : 10}, function (data) {
            if (data.success == true) {
                refreshDataList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}
function bindQueryDataMoreEvent() {
    $("#queryDataMore").click(function () {
        var keyword = $("#searchDataKeyword").val();
        var id = $("select[name='metaData']").select2("val");
        showMore($(this), '/monitor/queryData', {"error": keyword, "id" : id}, function(resultValue){
            refreshDataList(resultValue, true)
        });
    });
}
function refreshDataList(resultValue, append){
    var $dataList = $("#dataList");
    var $dataTotal = $("#dataTotal");
    var html = showData($dataList, resultValue.data, append);
    if(append){
        $dataList.append(html);
    }else{
        $dataList.html(html);
        $("#queryDataMore").attr("num", 1);
    }
    $dataTotal.html(resultValue.total);
    bindQueryDataDetailEvent();
}
function showData($dataList, arr, append){
    var html = '';
    var size = arr.length;
    if(size > 0){
        var start = append ? $dataList.find("tr").size() : 0;
        for(i = 0; i < size; i++) {
            html += '<tr>';
            html += '<td>' + (start + i + 1) + '</td>';
            html += '<td>' + arr[i].event + '</td>';
            html += '<td>' + (arr[i].success ? '<span class="label label-success">成功</span>' : '<span class="label label-warning">失败</span>') + '</td>';
            html += '<td>' + arr[i].error + '</td>';
            html += '<td>' + formatDate(arr[i].createTime) + '</td>';
            html += '<td><a json=' + arr[i].json + ' href="javascript:;" class="label label-info queryData">查看数据</a></td>';
            html += '</tr>';
        }
    }
    return html;
}

// 查看日志
function bindQueryLogEvent() {
    $("#queryLogBtn").click(function () {
        var keyword = $("#searchLogKeyword").val();
        doGetter('/monitor/queryLog', {"json": keyword, "pageNum" : 1, "pageSize" : 10}, function (data) {
            if (data.success == true) {
                refreshLogList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}
function bindQueryLogMoreEvent() {
    $("#queryLogMore").click(function () {
        var keyword = $("#searchLogKeyword").val();
        showMore($(this), '/monitor/queryLog', {"json": keyword}, function(resultValue){
            refreshLogList(resultValue, true)
        });
    });
}
function refreshLogList(resultValue, append){
    var $logList = $("#logList");
    var $logTotal = $("#logTotal");
    var html = showLog($logList, resultValue.data, append);
    if(append){
        $logList.append(html);
    }else{
        $logList.html(html);
        $("#queryLogMore").attr("num", 1);
    }
    $logTotal.html(resultValue.total);
}
function showLog($logList, arr, append){
    var size = arr.length;
    var html = '';
    if(size > 0){
        var start = append ? $logList.find("tr").size() : 0;
        for(i = 0; i < size; i++) {
            html += '<tr>';
            html += '<td>' + (start + i + 1) + '</td>';
            html += '<td>' + arr[i].json + '</td>';
            html += '<td>' + formatDate(arr[i].createTime) + '</td>';
            html += '</tr>';
        }
    }
    return html;
}

// 查看系统指标
function showSystemInfo(){
    doGetWithoutLoading("/app/health",{}, function (data) {
        var details = data.details;
        var html = showPoint("硬盘", details.diskSpace);

        doGetWithoutLoading("/app/metrics/jvm.threads.live",{}, function (data) {
            html += showSystemItem("线程活跃", data.measurements[0].value);
            doGetWithoutLoading("/app/metrics/jvm.threads.peak",{}, function (data) {
                html += showSystemItem("线程峰值", data.measurements[0].value);
                doGetWithoutLoading("/app/metrics/jvm.gc.pause",{}, function (data) {
                    var count =  data.measurements[0].value;
                    var time =  data.measurements[1].value;
                    time = time.toFixed(2);
                    var text = count+"次";
                    text += "，耗时:"+time + "秒";
                    html += showSystemItem("GC", text);
                    $("#systemList").html(html);
                });
            });
        });
    });

}

// CPU
function showCpu(){
    doGetWithoutLoading("/app/metrics/system.cpu.usage",{}, function (data) {
        var value = data.measurements[0].value * 100;
        value = value.toFixed(2);
        var option={
            title:{
                text:"CPU",
                x:'center',
                y: 'top'
            },
            tooltip : {
                formatter: "{a}: {c}%",
                position: 'top'
            },
            series: [
                {
                    name: "已用",
                    animation: true,
                    type: 'gauge',
                    min: 0,
                    max: 100,
                    splitNumber: 4,
                    axisLine: {            // 坐标轴线
                        lineStyle: {       // 属性lineStyle控制线条样式
                            color: [[0.1, '#d9534f'], [0.3, '#f0ad4e'],[0.8, '#5bc0de'],[1, '#5cb85c']],
                            width: 10
                        }
                    },
                    axisTick: {            // 坐标轴小标记
                        length: 15,        // 属性length控制线长
                        lineStyle: {       // 属性lineStyle控制线条样式
                            color: 'auto'
                        }
                    },
                    splitLine: {           // 分隔线
                        length: 20,         // 属性length控制线长
                        lineStyle: {       // 属性lineStyle（详见lineStyle）控制线条样式
                            color: 'auto'
                        }
                    },
                    detail: {formatter:'{value}%', fontSize:12, offsetCenter:[0,'65%']},
                    data: [{value: value, name: ''}]
                }
            ]
        };
        echarts.init(document.getElementById('cpuChart')).setOption(option);

    });
}

// 内存
function showMem(){
    doGetWithoutLoading("/app/metrics/jvm.memory.max",{}, function (data) {
        var max = data.measurements[0].value;
        max = (max / 1024 / 1024 / 1024).toFixed(2);
        doGetWithoutLoading("/app/metrics/jvm.memory.used",{}, function (data) {
            var used = data.measurements[0].value;
            used = (used / 1024 / 1024 / 1024).toFixed(2);
            doGetWithoutLoading("/app/metrics/jvm.memory.committed",{}, function (data) {
                var committed = data.measurements[0].value;
                committed = (committed / 1024 / 1024 / 1024).toFixed(2);

                var option = {
                    title : {
                        show:true,
                        text: '内存'+ max +'GB',
                        x:'center',
                        y: 'top'
                    },
                    tooltip : {
                        trigger: 'item',
                        formatter: "{b} : {c} GB"
                    },
                    series : [
                        {
                            name:'内存',
                            type:'pie',
                            color: function(params) {
                                // build a color map as your need.
                                var colorList = [
                                    '#60C0DD','#F0805A','#89DFAA'
                                ];
                                return colorList[params.dataIndex]
                            },
                            label:{
                                normal:{
                                    show:true,
                                    position:'inner',
                                    formatter:'{d}%'
                                }
                            },
                            data:[
                                {value:max,name:'总共'},
                                {value:used,name:'已用'},
                                {value:committed,name:'空闲'}
                            ]
                        }
                    ]
                };
                echarts.init(document.getElementById('memChart')).setOption(option);

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
    var precent = (threshold / total).toFixed(2);
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

function showSystemItem(title, value){
    var html = "";
    html += "<tr>";
    html += "   <td>" + title + "</td>";
    html += "   <td><span class='label label-success'>UP</span></td>";
    html += "   <td>"+value+"</td>";
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
        doLoader('/monitor?id=' + $id);
    });

    bindQueryLogEvent();
    bindQueryLogMoreEvent();
    bindQueryDataEvent();
    bindQueryDataMoreEvent();
    bindQueryDataDetailEvent();
    bindClearEvent($(".clearDataBtn"), "确认清空数据？", "清空数据成功!", "/monitor/clearData");
    bindClearEvent($(".clearLogBtn"), "确认清空日志？", "清空日志成功!", "/monitor/clearLog");

    showCpu();
    showMem();
    showSystemInfo();

    // 绑定回车事件
    $("#searchDataKeyword").keydown(function (e) {
        if (e.which == 13) {
            $("#queryDataBtn").trigger("click");
        }
    });
    $("#searchLogKeyword").keydown(function (e) {
        if (e.which == 13) {
            $("#queryLogBtn").trigger("click");
        }
    });

});