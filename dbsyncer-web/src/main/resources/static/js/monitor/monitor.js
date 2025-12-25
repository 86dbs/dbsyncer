
// 显示对话框
function showMessageDetail($message, icon, title) {
    $message.unbind('click').bind('click', function() {
        const content = $(this).text();
        if (content && content.trim()) {
            showConfirm({
                title: title,
                icon: icon,
                size: 'max',
                message: content
            });
        }
    });
}

// 打开重试页面
function showRetryDetail(metaId, messageId) {
    doLoader("/monitor/page/retry?metaId=" + metaId + "&messageId=" + messageId);
}

// 查看数据
function bindQueryDataEvent() {
    let pagination;
    let metaSelect;
    let statusSelect;
    let searchInput;

    function params() {
        return {
            "id": metaSelect.getValues()[0] || '',
            "status": statusSelect.getValues()[0] || '',
            "error": searchInput.getValue() || '',
        }
    }

    // 搜索函数
    function search() {
        pagination.doSearch(params());
    }

    // 搜索框输入事件
    searchInput = initSearch('searchData', search);

    // 结果下拉（跳过初始化回调，避免初始化时触发搜索）
    statusSelect = $('#searchDataStatus').dbSelect({
        type: 'single',
        onSelect: search
    });

    // 驱动下拉（跳过初始化回调，避免初始化时触发搜索）
    metaSelect = $('#searchDataMeta').dbSelect({
        type: 'single',
        onSelect: search
    });

    function renderDataState(success) {
        const state = {
            0: {
                class: 'badge-error',
                text: '失败',
            },
            1: {
                class: 'badge-success',
                text: '成功',
            }
        };
        const config = state[success];
        return `<span class="badge ${config.class}">${config.text}</span>`;
    }

    function renderDataButton(row) {
        const content = [];
        content.push(`<button class="table-action-btn view" title="查看数据" onclick="showDataDetail('${row.id}')">
                    <i class="fa fa-eye"></i>
                </button>`);
        // 如果失败，显示重试按钮
        if (row.success === 0) {
            let metaId = metaSelect.getValues()[0] || '';
            content.push(`<button class="table-action-btn play" title="重试" onclick="showRetryDetail('${metaId}','${row.id}')">
                            <i class="fa fa-refresh"></i>
                        </button>`);
        }
        return content.join(' ');
    }

    // 初始化分页管理器
    pagination = new PaginationManager({
        requestUrl: '/monitor/queryData',
        tableBodySelector: '#dataTableBody',
        params: params(),
        pageSize: 5,
        renderRow: function (d, index) {
            return `
                <tr>
                    <td>${index}</td>
                    <td>${d.targetTableName}</td>
                    <td>${d.event}</td>
                    <td>${renderDataState(d.success)}</td>
                    <td>
                        <span class="hover-underline cursor-pointer data-error">${d.error || ''}</span>
                    </td>
                    <td>${formatDate(d.createTime)}</td>
                    <td>
                        <div class="flex items-center gap-1">${renderDataButton(d)}</div>
                        <span id="${d.id}" class="hidden">${escapeHtml(d.json || '')}</span>
                    </td>
                </tr>`;
        },
        refreshCompleted: function () {
            showMessageDetail($('.data-error'), 'warning', '异常信息');
        },
        emptyHtml: '<td colspan="7" class="text-center"><i class="fa fa-exchange empty-icon"></i><p class="empty-text">暂无数据</p></td>'
    });

    $("#clearDataBtn").unbind('click').bind('click', function () {
        showConfirm({
            title: '确认清空数据？', icon: 'warning', size: 'large', confirmType: 'danger', onConfirm: function () {
                doPoster("/monitor/clearData", {id: metaSelect.getValues()[0] || ''}, function (response) {
                    if (response.success) {
                        bootGrowl('清空数据成功!', 'success');
                        search();
                    } else {
                        bootGrowl('清空数据失败: ' + response.message, 'danger');
                    }
                });
            }
        });
    })
}

// 将 JSON 对象转换为表格 HTML
function jsonToTable(jsonObj) {
    let $content = '<table class="table">';
    $content += '<thead><tr><th></th><th>字段</th><th>值</th></tr></thead>';
    $content += '<tbody>';
    let index = 1;
    $.each(jsonObj, function (name, value) {
        $content += '<tr>';
        $content += '<td>' + index + '</td>';
        $content += '<td>' + escapeHtml(name || '') + '</td>';
        $content += '<td class="white-space-none">' + escapeHtml(value || '') + '</td>';
        $content += '</tr>';
        index++;
    });
    $content += '</tbody>';
    $content += '</table>';
    return $content;
}

// 显示数据详情对话框
function showDataDetail(id) {
    const $element = $("#" + id);
    const content = $element.text();
    if (!content || !content.trim()) {
        return;
    }

    try {
        // 尝试解析 JSON
        const jsonObj = JSON.parse(content);
        showConfirm({
            title: '数据详情',
            icon: 'info',
            size: 'max',
            body: jsonToTable(jsonObj),
            confirmText: '关闭',
            confirmType: 'primary'
        });
    } catch (e) {
        // 如果解析失败，显示原始文本
        showConfirm({
            title: '数据详情',
            icon: 'info',
            size: 'max',
            message: content
        });
    }
}

// 查看日志
function bindQueryLogEvent() {
    // 初始化分页管理器
    const pagination = new PaginationManager({
        requestUrl: '/monitor/queryLog',
        tableBodySelector: '#logList',
        pageSize: 5,
        renderRow: function (row, index) {
            return `
                <tr>
                    <td>${index}</td>
                    <td><span class="hover-underline cursor-pointer log-detail">${row.json || ''}</span></td>
                    <td>${formatRelativeTime(row.createTime || '')}</td>
                </tr>
            `;
        },
        refreshCompleted: function () {
            showMessageDetail($('.log-detail'), 'info', '日志信息');
        }
    });
    // 搜索框输入事件
    initSearch('searchLog', function (searchKey) {
        pagination.doSearch({'json': searchKey});
    });

    $("#clearLogBtn").unbind('click').bind('click', function () {
        showConfirm({
            title: '确认清空日志？',
            icon: 'warning',
            size: 'large',
            confirmType: 'danger',
            onConfirm: function() {
                doPoster("/monitor/clearLog", {}, function (response) {
                    if (response.success) {
                        bootGrowl('清空日志成功!', 'success');
                        pagination.doSearch({});
                    } else {
                        bootGrowl('清空数据失败: ' + response.message, 'danger');
                    }
                });
            }
        });
    })
}

// 查看表执行器
function bindQueryActuatorEvent() {
    let pagination;
    let metaSelect;
    let searchInput;

    function params() {
        return {
            "id": metaSelect.getValues()[0] || '',
            "name": searchInput.getValue() || '',
        }
    }

    // 搜索函数
    function search() {
        pagination.doSearch(params());
    }

    // 驱动下拉（跳过初始化回调，避免初始化时触发搜索）
    metaSelect = $('#searchActuatorMeta').dbSelect({
        type: 'single',
        onSelect: search
    });

    // 搜索框输入事件
    searchInput = initSearch('searchActuator', search);

    // 初始化分页管理器
    pagination = new PaginationManager({
        requestUrl: '/monitor/queryActuator',
        tableBodySelector: '#actuatorList',
        pageSize: 5,
        params: params(),
        renderRow: function (row, index) {
            return `
                <tr>
                    <td>${index}</td>
                    <td>[${row.group || ''}]${row.metricName || ''}</td>
                    <td>${row.measurements[0].value || ''}</td>
                </tr>
            `;
        },
        emptyHtml: '<td colspan="3" class="text-center"><i class="fa fa-tasks empty-icon"></i><p class="empty-text">暂无数据</p></td>'
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
    function initLineChart(canvasId, label, color, solidFill) {
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
                        // suggestedMax: suggestedMax || 100,
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
     * 初始化所有图表
     */
    function initCharts() {
        // 仪表盘图表
        charts.queue = initGaugeChart('queueChart', '堆积数据', 320000);
        charts.storage = initGaugeChart('storageChart', '持久化', 50000);
        
        // 折线图
        charts.tps = initLineChart('tpsChart', 'TPS', 'rgba(245, 108, 108, 1)', false);
        charts.cpu = initLineChart('cpuChart', 'CPU使用率', 'rgba(82, 196, 26, 1)', false);
        charts.memory = initLineChart('memoryChart', '内存使用', 'rgba(24, 144, 255, 1)', true);
    }

    function updateMetricsTable(metrics){
        const trs = [];
        $.each(metrics, function (i) {
            trs.push(`<tr><td>${metrics[i].group}</td><td>${metrics[i].detail}</td></tr>`);
        });
        $("#metrics").html(trs);
    }

    function processBarState(percent) {
        if (percent >= 80) {
            return {
                value: 'danger',
                title: 'progress-value-danger'
            }
        }
        if (percent >= 60) {
            return {
                value: 'warning',
                title: 'progress-value-warning'
            }
        }
        return {
            value: 'success',
            title: 'progress-value-success'
        }
    }

    function updateCpuProcessBar(data){
        if (!data) {
            return;
        }
        let state = processBarState(data.totalPercent);
        $("#cpuProgressBar").html(`<div class="progress-header">
                <span class="progress-title">CPU使用率</span>
                <span class="progress-value ${state.title}">${data.totalPercent}%</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill ${state.value}" style="width: ${data.totalPercent}%"></div>
            </div>
            <div class="progress-detail">
                <span>核心数 <strong>${data.core}</strong></span>
                <span>用户态 <strong>${data.userPercent}%</strong></span>
                <span>系统态 <strong>${data.sysPercent}%</strong></span>
            </div>`);
    }

    function updateMemoryProcessBar(data){
        if (!data) {
            return;
        }
        let state = processBarState(data.totalPercent);
        $("#memoryProgressBar").html(`<div class="progress-header">
                <span class="progress-title">内存使用率</span>
                <span class="progress-value ${state.title}">${data.totalPercent}%</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill ${state.value}" style="width: ${data.totalPercent}%"></div>
            </div>
            <div class="progress-detail">
                <span>总内存 <strong>${data.sysTotal} GB</strong></span>
                <span>用户态 <strong>${data.jvmUsed} / ${data.jvmTotal} GB</strong></span>
                <span>系统态 <strong>${data.sysUsed} GB</strong></span>
            </div>`);
    }

    function updateDiskProcessBar(data){
        if (!data) {
            return;
        }
        let state = processBarState(data.usedPercent);
        $("#diskProgressBar").html(`<div class="progress-header">
                <span class="progress-title">磁盘使用率</span>
                <span class="progress-value ${state.title}">${data.usedPercent}%</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill ${state.value}" style="width: ${data.usedPercent}%"></div>
            </div>
            <div class="progress-detail">
                <span>总容量 <strong>${data.total} GB</strong></span>
                <span>已使用 <strong>${data.used} GB</strong></span>
            </div>`);
    }

    function updateMonitorData() {
        doGetter("/monitor/metric", {}, function (response) {
            if (response.success === true) {
                const r = response.data;
                // 更新折线图数据
                updateLineChart(charts.tps, r.tps.name, r.tps.value);
                $("#tps").text(r.tps.average > 0 ? '执行器TPS, 平均:'+ r.tps.average + '/秒' : '执行器TPS');
                updateLineChart(charts.cpu, r.cpu.name, r.cpu.value);
                updateLineChart(charts.memory, r.memory.name, r.memory.value);
                // 更新仪表盘
                updateGaugeChart(charts.queue, r.queueUp, r.queueCapacity);
                updateGaugeChart(charts.storage, r.storageQueueUp, r.storageQueueCapacity);
                updateMetricsTable(r.metrics);
                updateCpuProcessBar(r.cpu);
                updateMemoryProcessBar(r.memory);
                updateDiskProcessBar(r.disk);
            }
        });
    }
    
    // 页面加载完成后初始化
    initCharts();

    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/monitor');
    };

    // 立即执行一次更新
    updateMonitorData();

    bindQueryDataEvent();

    bindQueryLogEvent();

    bindQueryActuatorEvent();

    // 注册到全局定时刷新管理器
    PageRefreshManager.register(() => {
        updateMonitorData();
    });
});