/**
 * 监控页面 - 实时性能监控
 */
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
    function initLineChart(canvasId, label, color, suggestedMax) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;
        
        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: label,
                    data: [],
                    borderColor: color,
                    backgroundColor: color.replace('1)', '0.1)'),
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
     * 初始化面积图（TPS）
     */
    function initAreaChart(canvasId) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) return null;
        
        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'TPS',
                    data: [],
                    borderColor: 'rgba(24, 144, 255, 1)',
                    backgroundColor: 'rgba(24, 144, 255, 0.2)',
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
                            maxTicksLimit: 10
                        }
                    },
                    y: {
                        display: true,
                        beginAtZero: true,
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
                        padding: 12
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
        charts.tps = initAreaChart('tpsChart');
        charts.cpu = initLineChart('cpuChart', 'CPU使用率', 'rgba(82, 196, 26, 1)', 100);
        charts.memory = initLineChart('memoryChart', '内存使用', 'rgba(24, 144, 255, 1)', 1000);
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
            progressBar.className = 'monitor-progress-fill';
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
            valueElement.className = 'monitor-progress-value';
            if (percent >= 80) {
                valueElement.classList.add('monitor-progress-value-danger');
            } else if (percent >= 60) {
                valueElement.classList.add('monitor-progress-value-warning');
            } else {
                valueElement.classList.add('monitor-progress-value-success');
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
        
        // 更新统计卡片
        // $('#totalSpan').text(随机数);
        // $('#successSpan').text(随机数);
        // 等等...
        
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
    
    /**
     * 初始化事件监听
     */
    function initEvents() {
        // 查询数据按钮
        $('#queryDataBtn').on('click', function() {
            // TODO: 实现查询数据逻辑
            console.log('查询数据');
        });
        
        // 清空数据按钮
        $('.clearDataBtn').on('click', function() {
            if (confirm('确定要清空所有数据吗？此操作不可恢复！')) {
                // TODO: 实现清空数据逻辑
                console.log('清空数据');
            }
        });
        
        // 查询日志按钮
        $('#queryLogBtn').on('click', function() {
            // TODO: 实现查询日志逻辑
            console.log('查询日志');
        });
        
        // 清空日志按钮
        $('.clearLogBtn').on('click', function() {
            if (confirm('确定要清空所有日志吗？此操作不可恢复！')) {
                // TODO: 实现清空日志逻辑
                console.log('清空日志');
            }
        });
        
        // 查看数据详情
        $(document).on('click', '.queryData', function() {
            const jsonData = $(this).attr('json');
            // TODO: 弹窗显示JSON数据
            console.log('查看数据:', jsonData);
        });
        
        // 重试失败数据
        $(document).on('click', '.retryData', function() {
            const dataId = $(this).attr('id');
            // TODO: 实现重试逻辑
            console.log('重试数据:', dataId);
        });
        
        // 查看错误详情
        $(document).on('click', '.queryError', function() {
            const errorMsg = $(this).text();
            if (errorMsg && errorMsg !== 'null' && errorMsg.trim() !== '') {
                bootGrowl(errorMsg, 'danger');
            }
        });
        
        // 显示更多数据
        $('#queryDataMore').on('click', function() {
            // TODO: 实现加载更多逻辑
            console.log('加载更多数据');
        });
        
        // 显示更多日志
        $('#queryLogMore').on('click', function() {
            // TODO: 实现加载更多逻辑
            console.log('加载更多日志');
        });
    }
    
    /**
     * 初始化下拉框
     */
    function initSelects() {
        if (window.DBSyncerTheme && DBSyncerTheme.enhanceSelects) {
            DBSyncerTheme.enhanceSelects(document);
        }
    }
    
    // 页面加载完成后初始化
    initCharts();
    initEvents();
    initSelects();
    
    // 开始定时更新数据（每3秒更新一次）
    setInterval(updateMonitorData, 3000);
    
    // 立即执行一次更新
    updateMonitorData();
});