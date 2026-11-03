
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
function showRetryDetail(taskId, messageId) {
    doLoader("/monitor/page/retry?taskId=" + taskId + "&messageId=" + messageId);
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

// 查看执行器
function bindQueryActuatorEvent() {
    let pagination;
    let metaSelect;
    let searchInput;

    function params() {
        return {
            "id": metaSelect.getValues()[0] || '',
            "name": (searchInput && searchInput.getValue()) || '',
        }
    }

    // 搜索函数
    function search() {
        pagination.doSearch(params());
    }

    // 驱动下拉（远程分页搜索，跳过初始化回调避免重复请求）
    metaSelect = $('#searchActuatorMeta').dbSelect({
        type: 'single',
        remoteSearch: true,
        pageSize: 50,
        keywordDebounceMs: 300,
        loadOptions: function (query, onSuccess, onError) {
            doPoster('/monitor/queryMeta', {
                pageNum: query.pageNum,
                pageSize: query.pageSize,
                searchKey: query.searchKey || ''
            }, function (res) {
                if (res.success === true) {
                    const paging = res.data || {};
                    const rows = Array.isArray(paging.data) ? paging.data : [];
                    let converted = rows.map(function (row) {
                        const name = row.mappingName || '';
                        const model = row.model || '';
                        // 执行器指标按 Mapping/taskId 关联，不能用 Meta 主键
                        return {
                            label: name + ' (' + model + ')',
                            value: row.taskId || ''
                        };
                    });
                    if (Number(query.pageNum) === 1) {
                        converted = [{ label: '无', value: '' }].concat(converted);
                    }
                    onSuccess({
                        pageNum: paging.pageNum || query.pageNum,
                        pageSize: paging.pageSize || query.pageSize,
                        total: paging.total || 0,
                        data: converted
                    });
                } else {
                    bootGrowl(res.message || '加载驱动列表失败', 'danger');
                    if (typeof onError === 'function') {
                        onError();
                    }
                }
            });
        },
        onSelect: search
    });

    // 搜索框输入事件
    if (document.getElementById('searchActuator')) {
        searchInput = initSearch('searchActuator', search);
    }

    // 初始化分页管理器
    pagination = new PaginationManager({
        requestUrl: '/monitor/queryActuator',
        tableBodySelector: '#actuatorList',
        pageSize: 5,
        params: params(),
        showBoundaryButtons: false,
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
    return {
        search: function() {
            pagination.doSearch(params(), pagination.currentPage);
        }
    }
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
     * 初始化折线图（TPS、CPU、内存、堆积、持久化）
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
     * 更新折线图数据
     */
    function updateLineChart(chart, labels, data) {
        if (!chart) return;
        chart.data.labels = labels;
        chart.data.datasets[0].data = data;
        chart.update('none');
    }

    // 堆积/持久化：按 TPS 同一套时间点采样，缺测点用上一个值补齐
    const queueSamples = {};
    const storageSamples = {};

    function alignTrend(labels, samples, currentValue) {
        if (!labels || !labels.length) {
            return [];
        }
        const lastLabel = labels[labels.length - 1];
        samples[lastLabel] = Number(currentValue) || 0;

        const alive = {};
        for (let i = 0; i < labels.length; i++) {
            alive[labels[i]] = true;
        }
        Object.keys(samples).forEach(function (key) {
            if (!alive[key]) {
                delete samples[key];
            }
        });

        let last = 0;
        const values = [];
        for (let i = 0; i < labels.length; i++) {
            const key = labels[i];
            if (samples[key] !== undefined) {
                last = samples[key];
            }
            values.push(last);
        }
        return values;
    }

    /**
     * 初始化所有图表
     */
    function initCharts() {
        charts.tps = initLineChart('tpsChart', 'TPS', 'rgba(245, 108, 108, 1)', false);
        charts.cpu = initLineChart('cpuChart', 'CPU使用率', 'rgba(82, 196, 26, 1)', false);
        charts.memory = initLineChart('memoryChart', '内存使用', 'rgba(24, 144, 255, 1)', true);
        charts.queue = initLineChart('queueChart', '堆积', 'rgba(250, 173, 20, 1)', false);
        charts.storage = initLineChart('storageChart', '持久化', 'rgba(114, 46, 209, 1)', false);
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
                const tpsLabels = (r.tps && r.tps.name) || [];
                updateLineChart(charts.queue, tpsLabels, alignTrend(tpsLabels, queueSamples, r.queueUp));
                updateLineChart(charts.storage, tpsLabels, alignTrend(tpsLabels, storageSamples, r.storageQueueUp));
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

    const queryActuator = bindQueryActuatorEvent();

    // 注册到全局定时刷新管理器
    PageRefreshManager.register(() => {
        updateMonitorData();
        queryActuator.search();
    });
});