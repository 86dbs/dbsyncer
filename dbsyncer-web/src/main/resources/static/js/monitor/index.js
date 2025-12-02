/**
 * 监控页面 - 实时性能监控
 */

// 查看数据
function bindQueryDataEvent() {
    let pagination;
    let metaSelect;
    let statusSelect;
    let searchInput;

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
        content.push(`<button class="table-action-btn view" title="查看数据" onclick="queryData('${row?.json}')">
                    <i class="fa fa-eye"></i>
                </button>`);
        // 如果失败，显示重试按钮
        if (row.success === 0) {
            content.push(`<button class="table-action-btn play" title="重试" onclick="retryData('${row?.id}')">
                            <i class="fa fa-refresh"></i>
                        </button>`);
        }
        return content.join(' ');
    }

    // 初始化分页管理器
    pagination = new PaginationManager({
        requestUrl: '/monitor/queryData',
        tableBodySelector: '#dataTableBody',
        pageSize: 5,
        renderRow: function(d, index) {
            return `
                <tr>
                    <td class="text-center text-tertiary">${index}</td>
                    <td class="font-medium">${d?.targetTableName}</td>
                    <td>${d?.event}</td>
                    <td>${renderDataState(d.success)}</td>
                    <td>
                        <span class="text-secondary hover-underline queryError">${d?.error}</span>
                    </td>
                    <td class="text-sm text-secondary">${formatDate(d?.createTime)}</td>
                    <td>
                        <div class="flex items-center gap-1">${renderDataButton(d)}</div>
                    </td>
                </tr>`;
        },
        emptyHtml:'<td colspan="7" class="text-center"><i class="fa fa-exchange empty-icon"></i><p class="empty-text">暂无数据</p></td>'
    });
    // 搜索框输入事件
    searchInput = initSearch('searchData', search);
    // 结果下拉
    statusSelect = $('#searchDataStatus').dbSelect({
        type: 'single',
        onSelect: search
    });
    // 驱动下拉
    metaSelect = $('#searchDataMeta').dbSelect({
        type: 'single',
        onSelect: search
    });

    function search(data) {
        if (pagination && metaSelect && statusSelect && searchInput) {
            let metaIds = metaSelect.getValues();
            if (!metaIds) return;
            pagination.doSearch({
                "id": metaIds[0],
                "status": statusSelect.getValues()[0],
                "error": searchInput.getValue(),
            });
        } else {
            console.log("searchInput=" + searchInput);
            console.log("statusSelect=" + statusSelect);
            console.log("metaSelect=" + metaSelect);
        }
    }
}

function bindCleanData() {
    $("#clearDataBtn").unbind('click').bind('click', function () {
        let metaId = $(this).attr("metaId");
        showConfirm({
            title: '确认清空数据？', icon: 'warning', size: 'large', confirmType: 'danger', onConfirm: function () {
                doPoster("/monitor/clearData", {id: metaId}, function (response) {
                    if (response.success) {
                        bootGrowl('清空数据成功!', 'success');
                    } else {
                        bootGrowl('清空数据失败: ' + response.resultValue, 'danger');
                    }
                });
            }
        });
    })
}

// 查看日志
function bindQueryLogEvent() {
    // 初始化分页管理器
    const pagination = new PaginationManager({
        requestUrl: '/monitor/queryLog',
        tableBodySelector: '#logList',
        renderRow: function(row, index) {
            return `
                <tr>
                    <td>${index}</td>
                    <td>${escapeHtml(row.json || '')}</td>
                    <td>${formatRelativeTime(row.createTime || '')}</td>
                </tr>
            `;
        }
    });
    // 搜索框输入事件
    initSearch('searchLog', function (searchKey) {
        pagination.doSearch({'json': searchKey, 'pageNum': 1, 'pageSize': 10});
    });
}

function bindCleanLog() {
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
                    } else {
                        bootGrowl('清空数据失败: ' + response.resultValue, 'danger');
                    }
                });
            }
        });
    })
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

    bindQueryDataEvent();
    bindCleanData();

    bindQueryLogEvent();
    bindCleanLog();
});