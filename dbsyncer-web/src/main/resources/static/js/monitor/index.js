/**
 * 监控页面 - 实时性能监控
 */

// 显示更多
function showMore($this, $url, $params, $call) {
    $params.pageNum = parseInt($this.attr("num")) + 1;
    $params.pageSize = 10;
    doGetter($url, $params, function (data) {
        if (data.success === true) {
            if (data.resultValue.data.length > 0) {
                $this.attr("num", $params.pageNum);
            }
            $call(data.resultValue);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

function showLog($logList, arr, append) {
    const size = arr.length;
    let html = '';
    if (size > 0) {
        const start = append ? $logList.find("tr").size() : 0;
        for (i = 0; i < size; i++) {
            html += '<tr>';
            html += '<td>' + (start + i + 1) + '</td>';
            html += '<td>' + arr[i].json + '</td>';
            html += '<td>' + formatDate(arr[i].createTime) + '</td>';
            html += '</tr>';
        }
    }
    return html;
}

function refreshLogList(resultValue, append) {
    const $logList = $("#logList");
    const $logTotal = $("#logTotal");
    const html = showLog($logList, resultValue.data, append);
    if (append) {
        $logList.append(html);
    } else {
        $logList.html(html);
        $("#queryLogMore").attr("num", 1);
    }
    $logTotal.html(resultValue.total);
}

function showData($dataList, arr, append) {
    let html = '';
    const size = arr.length;
    if (size > 0) {
        const start = append ? $dataList.find("tr").size() : 0;
        for (i = 0; i < size; i++) {
            html += '<tr>';
            html += '<td>' + (start + i + 1) + '</td>';
            html += '<td>' + arr[i].targetTableName + '</td>';
            html += '<td>' + arr[i].event + '</td>';
            html += '<td>' + (arr[i].success ? '<span class="label label-success">成功</span>' : '<span class="label label-warning">失败</span>') + '</td>';
            html += '<td style="max-width:100px;" class="dbsyncer_over_hidden"><a href="javascript:;" class="dbsyncer_pointer queryError">' + arr[i].error + '</a></td>';
            html += '<td>' + formatDate(arr[i].createTime) + '</td>';
            html += '<td><div class="hidden">' + arr[i].json + '</div><a href="javascript:;" class="label label-info queryData">查看数据</a>&nbsp;';
            html += (arr[i].success ? '' : '<a id="' + arr[i].id + '" href="javascript:;" class="label label-warning retryData">重试</a>');
            html += '</td>';
            html += '</tr>';
        }
    }
    return html;
}

function refreshDataList(resultValue, append) {
    const $dataList = $("#dataList");
    const $dataTotal = $("#dataTotal");
    const html = showData($dataList, resultValue.data, append);
    if (append) {
        $dataList.append(html);
    } else {
        $dataList.html(html);
        $("#queryDataMore").attr("num", 1);
    }
    $dataTotal.html(resultValue.total);
    // bindQueryDataDetailEvent();
    // bindQueryDataRetryEvent();
    // bindQueryErrorDetailEvent();
}

// 查看日志
function bindQueryLogEvent() {
    // 初始化分页管理器
    const pagination = new PaginationManager({
        requestUrl: '/monitor/queryLog',
        tableBodySelector: '#logList',
        paginationSelector: '#logPagination',
        renderRow: function(row, index) {
            return `
                <tr>
                    <td>${index}</td>
                    <td>${escapeHtml(row.json || '')}</td>
                    <td>${formatDate(row.createTime || '')}</td>
                </tr>
            `;
        }
    });
    // 搜索框输入事件
    initSearch('searchLog', function (searchKey) {
        pagination.doSearch({'json': searchKey, 'pageNum': 1, 'pageSize': 10});
    });
}

function bindQueryLogMoreEvent() {
    $("#queryLogMore").click(function () {
        const keyword = $("#searchLogKeyword").val();
        showMore($(this), '/monitor/queryLog', {"json": keyword}, function (resultValue) {
            refreshLogList(resultValue, true)
        });
    });
}

// 查看数据
function bindQueryDataEvent() {
    initSearch("searchData", function(value){
        const id = $("#searchMetaData").val();
        const success = $("#searchDataSuccess").val();
        doGetter('/monitor/queryData', {
            "error": value,
            "success": success,
            "id": id,
            "pageNum": 1,
            "pageSize": 10
        }, function (data) {
            if (data.success === true) {
                refreshDataList(data.resultValue);
            } else {
                bootGrowl(data.resultValue, "danger");
            }
        });
    });
}

function bindQueryDataMoreEvent() {
    $("#queryDataMore").click(function () {
        const keyword = $("#searchDataKeyword").val();
        const id = $("#searchMetaData").val();
        const success = $("#searchDataSuccess").val();
        showMore($(this), '/monitor/queryData', {
            "error": keyword,
            "success": success,
            "id": id
        }, function (resultValue) {
            refreshDataList(resultValue, true)
        });
    });
}

// 清空数据
function bindClearEvent($btn, $title, $msg, $url, $callback) {
    $btn.click(function () {
        if (confirm($title)) {
            const $id = null != $callback ? $callback() : $(this).attr("metaId");
            const data = {"id": $id};
            doPoster($url, data, function (data) {
                if (data.success === true) {
                    bootGrowl($msg, "success");
                    doLoader('/monitor?id=' + $id);
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });
}

$(function () {
    // 图表实例
    let charts = {
        queue: null,
        storage: null,
        tps: null,
        cpu: null,
        memory: null
    };
    
    // 数据历史记录（最多保留50个点）
    const MAX_DATA_POINTS = 50;
    let dataHistory = {
        queue: [],
        storage: [],
        tps: [],
        cpu: [],
        memory: [],
        labels: []
    };
    
    // Chart.js 默认配置
    Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
    Chart.defaults.font.size = 12;
    Chart.defaults.color = '#8c8c8c';
    
    /**
     * 初始化仪表盘图表（堆积数据、持久化）
     */
    function initGaugeChart(canvasId, label, maxValue) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;
        
        return new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['已用', '剩余'],
                datasets: [{
                    data: [0, maxValue],
                    backgroundColor: [
                        'rgba(24, 144, 255, 0.8)',
                        'rgba(240, 240, 240, 0.5)'
                    ],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '75%',
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        enabled: true,
                        callbacks: {
                            label: function(context) {
                                return context.label + ': ' + context.parsed.toLocaleString();
                            }
                        }
                    }
                }
            },
            plugins: [{
                id: 'centerText',
                beforeDraw: function(chart) {
                    const width = chart.width;
                    const height = chart.height;
                    const ctx = chart.ctx;
                    const data = chart.data.datasets[0].data;
                    const total = data[0] + data[1];
                    const value = data[0];
                    
                    ctx.restore();
                    ctx.font = 'bold 24px sans-serif';
                    ctx.textBaseline = 'middle';
                    ctx.fillStyle = '#262626';
                    
                    const text = value.toLocaleString();
                    const textX = Math.round((width - ctx.measureText(text).width) / 2);
                    const textY = height / 2;
                    
                    ctx.fillText(text, textX, textY);
                    ctx.save();
                }
            }]
        });
    }
    
    /**
     * 初始化折线图（TPS、CPU、内存）
     */
    function initLineChart(canvasId, label, color, suggestedMax, solidFill) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;
        
        // 如果是实心填充，使用不透明颜色；否则使用半透明
        const bgColor = solidFill ? color.replace('1)', '0.6)') : color.replace('1)', '0.1)');
        
        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: label,
                    data: [],
                    borderColor: color,
                    backgroundColor: bgColor,
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: {
                    x: {
                        display: true,
                        grid: {
                            display: false
                        },
                        ticks: {
                            maxTicksLimit: 8
                        }
                    },
                    y: {
                        display: true,
                        beginAtZero: true,
                        suggestedMax: suggestedMax || 100,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.05)'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        enabled: true,
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        padding: 12,
                        titleColor: '#fff',
                        bodyColor: '#fff'
                    }
                }
            }
        });
    }
    
    /**
     * 更新仪表盘数据
     */
    function updateGaugeChart(chart, value, maxValue) {
        if (!chart) return;
        chart.data.datasets[0].data = [value, Math.max(0, maxValue - value)];
        chart.update('none'); // 无动画更新
    }
    
    /**
     * 更新折线图数据
     */
    function updateLineChart(chart, labels, data) {
        if (!chart) return;
        chart.data.labels = labels;
        chart.data.datasets[0].data = data;
        chart.update('none');
    }
    
    /**
     * 添加数据点到历史记录
     */
    function addDataPoint(type, value) {
        if (dataHistory[type].length >= MAX_DATA_POINTS) {
            dataHistory[type].shift();
        }
        dataHistory[type].push(value);
    }
    
    /**
     * 获取当前时间标签
     */
    function getTimeLabel() {
        const now = new Date();
        return now.getHours().toString().padStart(2, '0') + ':' + 
               now.getMinutes().toString().padStart(2, '0') + ':' + 
               now.getSeconds().toString().padStart(2, '0');
    }
    
    /**
     * 初始化所有图表
     */
    function initCharts() {
        // 仪表盘图表
        charts.queue = initGaugeChart('queueChart', '堆积数据', 320000);
        charts.storage = initGaugeChart('storageChart', '持久化', 50000);
        
        // 折线图
        charts.tps = initLineChart('tpsChart', 'TPS', 'rgba(245, 108, 108, 1)', 1000, false);
        charts.cpu = initLineChart('cpuChart', 'CPU使用率', 'rgba(82, 196, 26, 1)', 100, false);
        charts.memory = initLineChart('memoryChart', '内存使用', 'rgba(24, 144, 255, 1)', 1000, true);
    }
    
    /**
     * 更新右侧进度条
     */
    function updateProgressBar(elementId, valueId, percent, type) {
        const progressBar = document.getElementById(elementId);
        const valueElement = document.getElementById(valueId);
        
        if (progressBar) {
            progressBar.style.width = percent + '%';
            // 根据百分比动态调整颜色
            progressBar.className = 'progress-fill';
            if (percent >= 80) {
                progressBar.classList.add('danger');
            } else if (percent >= 60) {
                progressBar.classList.add('warning');
            } else {
                progressBar.classList.add('success');
            }
        }
        
        if (valueElement) {
            valueElement.textContent = percent + '%';
            // 根据百分比动态调整文字颜色
            valueElement.className = 'progress-value';
            if (percent >= 80) {
                valueElement.classList.add('progress-value-danger');
            } else if (percent >= 60) {
                valueElement.classList.add('progress-value-warning');
            } else {
                valueElement.classList.add('progress-value-success');
            }
        }
    }
    
    /**
     * 模拟数据更新（实际应该从后端API获取）
     */
    function updateMonitorData() {
        // 获取当前时间标签
        const timeLabel = getTimeLabel();
        if (dataHistory.labels.length >= MAX_DATA_POINTS) {
            dataHistory.labels.shift();
        }
        dataHistory.labels.push(timeLabel);
        
        // 模拟数据（实际应该通过AJAX从后端获取）
        const queueValue = Math.floor(Math.random() * 50000);
        const storageValue = Math.floor(Math.random() * 10000);
        const tpsValue = Math.floor(Math.random() * 800 + 200);
        const cpuValue = Math.floor(Math.random() * 30 + 20);
        const memoryValue = Math.floor(Math.random() * 300 + 400);
        
        // 右侧进度条数据（模拟）
        const cpuPercent = Math.floor(Math.random() * 30 + 50); // 50-80%
        const memoryPercent = Math.floor(Math.random() * 20 + 60); // 60-80%
        const diskPercent = Math.floor(Math.random() * 20 + 30); // 30-50%
        
        // 更新仪表盘
        updateGaugeChart(charts.queue, queueValue, 320000);
        updateGaugeChart(charts.storage, storageValue, 50000);
        
        // 更新折线图数据
        addDataPoint('tps', tpsValue);
        addDataPoint('cpu', cpuValue);
        addDataPoint('memory', memoryValue);
        
        updateLineChart(charts.tps, dataHistory.labels, dataHistory.tps);
        updateLineChart(charts.cpu, dataHistory.labels, dataHistory.cpu);
        updateLineChart(charts.memory, dataHistory.labels, dataHistory.memory);
        
        // 更新右侧进度条
        updateProgressBar('cpuProgressBar', 'cpuPercentValue', cpuPercent);
        updateProgressBar('memoryProgressBar', 'memoryPercentValue', memoryPercent);
        updateProgressBar('diskProgressBar', 'diskPercentValue', diskPercent);
        
        // 更新详细信息（可选）
        $('#cpuUser').text(Math.floor(cpuPercent * 0.65) + '%');
        $('#cpuSystem').text(Math.floor(cpuPercent * 0.35) + '%');
        
        const memoryUsed = Math.floor(32 * memoryPercent / 100);
        $('#memoryUsed').text(memoryUsed + ' GB');
        
        const diskUsed = Math.floor(1.2 * 1024 * diskPercent / 100);
        $('#diskUsed').text(diskUsed + ' GB');
    }
    
    // 页面加载完成后初始化
    initCharts();
    
    // 开始定时更新数据（每3秒更新一次）
    setInterval(updateMonitorData, 3000);
    
    // 立即执行一次更新
    updateMonitorData();

    bindQueryLogEvent();
    bindQueryLogMoreEvent();
    bindQueryDataEvent();
    bindQueryDataMoreEvent();
    bindClearEvent($("#clearDataBtn"), "确认清空数据？", "清空数据成功!", "/monitor/clearData", function (){
        // todo 刷新驱动数据
    });
    bindClearEvent($("#clearLogBtn"), "确认清空日志？", "清空日志成功!", "/monitor/clearLog");

    $('#searchDataStatus').dbSelect({
        type: 'single'
    });
});