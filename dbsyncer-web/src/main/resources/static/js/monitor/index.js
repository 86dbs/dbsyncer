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
        var json = $(this).parent().find("div").text();
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

// 查看详细数据日志
function bindQueryErrorDetailEvent() {
    var $queryData = $(".queryError");
    $queryData.unbind("click");
    $queryData.click(function () {
        var json = $(this).text();
        var html = '<div class="row driver_break_word">' + json + '</div>';
        BootstrapDialog.show({
            title: "异常详细",
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
        var id = $("#searchMetaData").select2("val");
        var success = $("#searchDataSuccess").select2("val");
        doGetter('/monitor/queryData', {"error": keyword, "success": success, "id" : id, "pageNum" : 1, "pageSize" : 10}, function (data) {
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
        var id = $("#searchMetaData").select2("val");
        var success = $("#searchDataSuccess").select2("val");
        showMore($(this), '/monitor/queryData', {"error": keyword, "success": success, "id" : id}, function(resultValue){
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
    bindQueryErrorDetailEvent();
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
            html += '<td style="max-width:100px;" class="dbsyncer_over_hidden"><a href="javascript:;" class="dbsyncer_pointer queryError">' + arr[i].error + '</a></td>';
            html += '<td>' + formatDate(arr[i].createTime) + '</td>';
            html += '<td><a href="javascript:;" class="label label-info queryData">查看数据</a><div class="hidden">' + arr[i].json + '</div></td>';
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

// 堆积数据
function showQueueChart(taskNumber){
    var option={
        title:{
            text:"堆积数据",
            x:'center',
            y: 'top'
        },
        tooltip : {
            formatter: "{a}: {c}",
            position: 'top'
        },
        series: [
            {
                name: "待处理",
                animation: true,
                type: 'gauge',
                min: 0,
                max: 1000,
                splitNumber: 5,
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
                detail: {fontSize:12, offsetCenter:[0,'65%']},
                data: [{value: taskNumber, name: ''}]
            }
        ]
    };
    echarts.init(document.getElementById('queueChart')).setOption(option);
}

// 事件分类
function showEventChart(ins, upd, del){
    var option = {
        title : {
            show:true,
            text: '事件分类',
            x:'center',
            y: 'top'
        },
        tooltip : {
            trigger: 'item',
            formatter: "{b} : {c}"
        },
        series : [
            {
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
                    {value:upd, name:'更新'},
                    {value:del, name:'删除'},
                    {value:ins, name:'插入'}
                ]
            }
        ]
    };
    echarts.init(document.getElementById('eventChart')).setOption(option);
}

// 统计成功失败
function showTotalChart(success, fail){
    var option = {
        title : {
            show:true,
            text: '已完成数据',
            x:'center',
            y: 'top'
        },
        tooltip : {
            trigger: 'item',
            formatter: "{b} : {c}"
        },
        series : [
            {
                type:'pie',
                color: function(params) {
                    // build a color map as your need.
                    var colorList = [
                        '#60C0DD','#F0805A'
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
                    {value:success, name:'成功'},
                    {value:fail, name:'失败'}
                ]
            }
        ]
    };
    echarts.init(document.getElementById('totalChart')).setOption(option);
}

$(function () {
    // 初始化select2插件
    $(".select-control").select2({
        width: "100%",
        theme: "classic"
    });

    // 连接器类型切换事件
    $("#searchMetaData").change(function () {
        var $id = $(this).val();
        doLoader('/monitor?id=' + $id);
    });
    // 数据状态切换事件
    $("#searchDataSuccess").change(function () {
        $("#queryDataBtn").click();
    });

    bindQueryLogEvent();
    bindQueryLogMoreEvent();
    bindQueryDataEvent();
    bindQueryDataMoreEvent();
    bindQueryDataDetailEvent();
    bindQueryErrorDetailEvent();
    bindClearEvent($(".clearDataBtn"), "确认清空数据？", "清空数据成功!", "/monitor/clearData");
    bindClearEvent($(".clearLogBtn"), "确认清空日志？", "清空日志成功!", "/monitor/clearLog");

    doGetWithoutLoading("/monitor/queryAppReportMetric",{}, function (data) {
        if (data.success == true) {
            var report = data.resultValue;
            showTotalChart(report.success, report.fail);
            showEventChart(report.insert, report.update, report.delete);
            showQueueChart(report.taskNumber);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });

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