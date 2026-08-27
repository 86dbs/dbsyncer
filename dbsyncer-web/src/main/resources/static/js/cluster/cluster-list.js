/**
 * 集群管理列表
 */
(function (window) {
    'use strict';

    /** 近 1 分钟：监控刷新 5s，取最近 12 个点 */
    var ONE_MIN_POINTS = 12;
    var charts = {
        cpu: null,
        memory: null,
        queue: null,
        tps: null
    };
    var chartHistoryByNode = {};

    var clusterEnabled = false;
    var currentIsLeader = false;
    var currentNodeId = '';
    var selectedChartNodeId = '';
    var chartNodeSelect = null;
    var skipChartNodeSelectEvent = false;
    var pagination = null;
    var metricsByNodeId = {};

    function initClusterList(options) {
        options = options || {};
        clusterEnabled = options.clusterEnabled === true;
        currentIsLeader = options.currentIsLeader === true;
        currentNodeId = options.currentNodeId || '';
        selectedChartNodeId = currentNodeId;
        metricsByNodeId = {};
        chartHistoryByNode = {};
        chartNodeSelect = null;
        skipChartNodeSelectEvent = false;

        window.backIndexPage = function () {
            doLoader('/cluster/list');
        };

        bindClusterActions();
        destroyCharts();
        if (clusterEnabled) {
            initCharts();
            bindChartNodeSelect();
        }

        pagination = new PaginationManager({
            requestUrl: '/cluster/query',
            tableBodySelector: '#clusterTableBody',
            renderRow: renderClusterRow,
            storageKey: 'cluster-list'
        });

        loadNodeMetrics(true);
        if (typeof PageRefreshManager !== 'undefined' && PageRefreshManager.register) {
            PageRefreshManager.register(function () {
                loadNodeMetrics(false);
            });
        }
    }

    function destroyCharts() {
        Object.keys(charts).forEach(function (key) {
            if (charts[key] && typeof charts[key].destroy === 'function') {
                charts[key].destroy();
            }
            charts[key] = null;
        });
    }

    function initLineChart(canvasId, label, color, solidFill) {
        if (typeof Chart === 'undefined') {
            return null;
        }
        var canvas = document.getElementById(canvasId);
        if (!canvas) {
            return null;
        }
        var bgColor = solidFill ? color.replace('1)', '0.6)') : color.replace('1)', '0.1)');
        return new Chart(canvas, {
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
                        grid: {display: false},
                        ticks: {maxTicksLimit: 6}
                    },
                    y: {
                        display: true,
                        beginAtZero: true,
                        grid: {color: 'rgba(0, 0, 0, 0.05)'}
                    }
                },
                plugins: {
                    legend: {display: false},
                    tooltip: {
                        enabled: true,
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        padding: 12
                    }
                }
            }
        });
    }

    function initCharts() {
        charts.cpu = initLineChart('clusterCpuChart', 'CPU', 'rgba(82, 196, 26, 1)', false);
        charts.memory = initLineChart('clusterMemoryChart', '内存', 'rgba(24, 144, 255, 1)', true);
        charts.queue = initLineChart('clusterQueueChart', '堆积数', 'rgba(250, 173, 20, 1)', false);
        charts.tps = initLineChart('clusterTpsChart', 'TPS', 'rgba(245, 108, 108, 1)', false);
    }

    function updateLineChart(chart, labels, data) {
        if (!chart) {
            return;
        }
        chart.data.labels = labels || [];
        chart.data.datasets[0].data = data || [];
        chart.update('none');
    }

    function takeLast(list, size) {
        if (!list || !list.length) {
            return [];
        }
        if (list.length <= size) {
            return list.slice();
        }
        return list.slice(list.length - size);
    }

    function pad2(n) {
        return n < 10 ? '0' + n : String(n);
    }

    function nowTimeLabel() {
        var d = new Date();
        return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
    }

    function ensureNodeHistory(nodeId) {
        if (!chartHistoryByNode[nodeId]) {
            chartHistoryByNode[nodeId] = {
                cpu: {name: [], value: []},
                memory: {name: [], value: []},
                queue: {name: [], value: []},
                tps: {name: [], value: []}
            };
        }
        return chartHistoryByNode[nodeId];
    }

    function pushHistoryPoint(history, value) {
        history.name.push(nowTimeLabel());
        history.value.push(Number(value) || 0);
        while (history.name.length > ONE_MIN_POINTS) {
            history.name.shift();
            history.value.shift();
        }
    }

    function memoryToMb(metric) {
        if (!metric || metric.memoryUsed === null || metric.memoryUsed === undefined || metric.memoryUsed === '') {
            return 0;
        }
        return Number(metric.memoryUsed) * 1024;
    }

    function averageTps(values) {
        if (!values || !values.length) {
            return 0;
        }
        var sum = 0;
        values.forEach(function (item) {
            sum += Number(item) || 0;
        });
        return Math.floor(sum / values.length);
    }

    function clearCharts() {
        updateLineChart(charts.cpu, [], []);
        updateLineChart(charts.memory, [], []);
        updateLineChart(charts.queue, [], []);
        updateLineChart(charts.tps, [], []);
        $('#clusterTpsTitle').text('执行器TPS');
    }

    function formatNodeOptionLabel(item) {
        var name = item.name || item.nodeId || '';
        return item.local ? (name + ' (本机)') : name;
    }

    function refreshChartNodeSelect(nodes) {
        if (!chartNodeSelect || !nodes || !nodes.length) {
            return;
        }
        var prev = selectedChartNodeId;
        var data = [];
        nodes.forEach(function (item) {
            if (!item || !item.nodeId) {
                return;
            }
            data.push({
                label: formatNodeOptionLabel(item),
                value: item.nodeId
            });
        });
        if (!data.length) {
            return;
        }
        if (prev && metricsByNodeId[prev]) {
            selectedChartNodeId = prev;
        } else if (currentNodeId && metricsByNodeId[currentNodeId]) {
            selectedChartNodeId = currentNodeId;
        } else {
            selectedChartNodeId = data[0].value;
        }
        skipChartNodeSelectEvent = true;
        chartNodeSelect.setData(data);
        chartNodeSelect.setValues([selectedChartNodeId], true);
        skipChartNodeSelectEvent = false;
    }

    function bindChartNodeSelect() {
        var $sel = $('#clusterChartNodeSelect');
        if (!$sel.length || typeof $sel.dbSelect !== 'function') {
            return;
        }
        var existing = $sel.data('dbSelect');
        if (existing && typeof existing.destroy === 'function') {
            existing.destroy();
        }
        chartNodeSelect = $sel.dbSelect({
            type: 'single',
            onSelect: function (values) {
                if (skipChartNodeSelectEvent) {
                    return;
                }
                selectedChartNodeId = (values && values[0]) || '';
                loadCharts();
            }
        });
    }

    function loadCharts() {
        if (!clusterEnabled || !selectedChartNodeId) {
            return;
        }
        var metric = metricsByNodeId[selectedChartNodeId];
        if (metric && metric.local) {
            loadLocalCharts();
            return;
        }
        if (metric && metric.reachable) {
            loadRemoteCharts(metric);
            return;
        }
        clearCharts();
    }

    function loadRemoteCharts(metric) {
        var history = ensureNodeHistory(selectedChartNodeId);
        pushHistoryPoint(history.cpu, metric.cpuPercent);
        pushHistoryPoint(history.memory, memoryToMb(metric));
        pushHistoryPoint(history.queue, metric.queueUp);
        pushHistoryPoint(history.tps, Math.floor(metric.tps || 0));
        updateLineChart(charts.cpu, history.cpu.name.slice(), history.cpu.value.slice());
        updateLineChart(charts.memory, history.memory.name.slice(), history.memory.value.slice());
        updateLineChart(charts.queue, history.queue.name.slice(), history.queue.value.slice());
        updateLineChart(charts.tps, history.tps.name.slice(), history.tps.value.slice());
        var avg = averageTps(history.tps.value);
        $('#clusterTpsTitle').text(avg > 0 ? ('执行器TPS, 平均:' + avg + '/秒') : '执行器TPS');
    }

    function loadLocalCharts() {
        doGetter('/monitor/metric', {}, function (res) {
            if (res.success !== true || !res.data) {
                return;
            }
            var r = res.data;
            if (r.cpu) {
                updateLineChart(charts.cpu, takeLast(r.cpu.name, ONE_MIN_POINTS), takeLast(r.cpu.value, ONE_MIN_POINTS));
            }
            if (r.memory) {
                updateLineChart(charts.memory, takeLast(r.memory.name, ONE_MIN_POINTS), takeLast(r.memory.value, ONE_MIN_POINTS));
            }
            if (r.queue) {
                updateLineChart(charts.queue, r.queue.name || [], r.queue.value || []);
            }
            if (r.tps) {
                updateLineChart(charts.tps, r.tps.name || [], r.tps.value || []);
                var title = r.tps.average > 0
                    ? ('执行器TPS, 平均:' + r.tps.average + '/秒')
                    : '执行器TPS';
                $('#clusterTpsTitle').text(title);
            }
        });
    }

    function bindClusterActions() {
        $('#clusterTableBody').on('click', '[data-action]', function () {
            var $btn = $(this);
            var id = $btn.attr('data-id');
            var action = $btn.attr('data-action');
            if (action === 'transfer') {
                transferNode(id);
                return;
            }
            if (action === 'remove') {
                removeClusterNode(id);
            }
        });
    }

    function metricOf(nodeId) {
        return metricsByNodeId[nodeId] || null;
    }

    function formatPercent(value) {
        if (value === null || value === undefined || value === '') {
            return '-';
        }
        return Number(value).toFixed(2).replace(/\.00$/, '') + '%';
    }

    function formatUsedTotal(used, total, suffix) {
        if (used === null || used === undefined || total === null || total === undefined) {
            return '-';
        }
        var u = Number(used);
        var t = Number(total);
        if (isNaN(u) || isNaN(t)) {
            return '-';
        }
        var unit = suffix || 'G';
        return trimNum(u) + unit + '/' + trimNum(t) + unit;
    }

    function trimNum(n) {
        if (Math.abs(n - Math.round(n)) < 0.05) {
            return String(Math.round(n));
        }
        return n.toFixed(1);
    }

    function formatDash(value) {
        if (value === null || value === undefined || value === '') {
            return '-';
        }
        return String(value);
    }

    function formatMetric(m, getter) {
        if (!m || !m.reachable) {
            return '-';
        }
        return getter(m);
    }

    function buildSsoConsoleUrl(item) {
        if (!item || !item.ip || !item.httpPort) {
            return '';
        }
        // 禁止把 http:// 放进 query：Spring StrictHttpFirewall 会因 // 直接 400
        var target = item.ip + ':' + item.httpPort;
        return '/sso/redirect?target=' + encodeURIComponent(target) + '&redirect=' + encodeURIComponent('/');
    }

    function renderClusterRow(item) {
        var networkText = item.networkOk ? '正常' : '异常';
        var networkClass = item.networkOk ? 'text-success' : 'text-error';
        var name = item.name || item.id || '';
        var localMark = item.local ? ' (本机)' : '';
        var m = metricOf(item.id);
        var fullWorkItems = formatMetric(m, function (metric) {
            return formatDash(metric.fullWorkItemCount);
        });
        var incremental = formatMetric(m, function (metric) {
            return formatDash(metric.incrementalCount);
        });
        var tps = formatMetric(m, function (metric) {
            return formatDash(Math.floor(metric.tps || 0));
        });
        var queueUp = formatMetric(m, function (metric) {
            return formatDash(metric.queueUp);
        });
        var storageQueueUp = formatMetric(m, function (metric) {
            return formatDash(metric.storageQueueUp);
        });
        var cpu = m && m.reachable ? formatPercent(m.cpuPercent) : '-';
        var memory = m && m.reachable ? formatUsedTotal(m.memoryUsed, m.memoryTotal, 'G') : '-';
        var threads = m && m.reachable ? formatDash(m.threadLive) : '-';
        var disk = m && m.reachable ? formatUsedTotal(m.diskUsed, m.diskTotal, 'G') : '-';
        var buttons = [];
        if (clusterEnabled && currentIsLeader && !item.local) {
            var nodeId = escapeHtml(item.id || '');
            if (item.status === 1 && !item.leader) {
                buttons.push(
                    '<button type="button" class="table-action-btn view" title="切换为Leader" data-id="'
                    + nodeId + '" data-action="transfer"><i class="fa fa-flag"></i></button>'
                );
            }
            if (!item.leader) {
                buttons.push(
                    '<button type="button" class="btn btn-danger btn-sm" title="删除" data-id="'
                    + nodeId + '" data-action="remove">删除</button>'
                );
            }
        }
        var actions = buttons.length > 0
            ? '<div class="flex items-center">' + buttons.join('') + '</div>'
            : '-';
        var nameHtml = escapeHtml(name);
        if (clusterEnabled && !item.local) {
            var consoleUrl = buildSsoConsoleUrl(item);
            if (consoleUrl) {
                nameHtml = '<a class="text-primary hover-underline" title="打开控制台" href="'
                    + consoleUrl + '">' + nameHtml + '</a>';
            }
        }
        return '<tr>'
            + '<td>' + nameHtml + localMark + '</td>'
            + '<td>' + escapeHtml(item.roleName || '') + '</td>'
            + '<td>' + escapeHtml(item.statusName || '') + '</td>'
            + '<td class="' + networkClass + '">' + networkText + '</td>'
            + '<td>' + fullWorkItems + '</td>'
            + '<td>' + incremental + '</td>'
            + '<td>' + tps + '</td>'
            + '<td>' + queueUp + '</td>'
            + '<td>' + storageQueueUp + '</td>'
            + '<td>' + cpu + '</td>'
            + '<td>' + memory + '</td>'
            + '<td>' + threads + '</td>'
            + '<td>' + disk + '</td>'
            + '<td>' + actions + '</td>'
            + '</tr>';
    }

    function transferNode(id) {
        showConfirm({
            title: '确定将该节点切换为 Leader？',
            icon: 'warning',
            onConfirm: function () {
                doPoster('/cluster/transfer', {id: id}, function (res) {
                    if (res.success === true) {
                        bootGrowl('已发起切换', 'success');
                        doLoader('/cluster/list');
                    } else {
                        bootGrowl(res.message || '切换失败', 'danger');
                    }
                });
            }
        });
    }

    function removeClusterNode(id) {
        showConfirm({
            title: '确定删除该节点？',
            icon: 'warning',
            confirmType: 'danger',
            onConfirm: function () {
                doPoster('/cluster/remove', {id: id}, function (res) {
                    if (res.success === true) {
                        bootGrowl('已删除', 'success');
                        doLoader('/cluster/list');
                    } else {
                        bootGrowl(res.message || '删除失败', 'danger');
                    }
                });
            }
        });
    }

    function updateTaskSummary(overview) {
        if (!overview) {
            $('#clusterTpsTotal').text('-');
            $('#clusterWorkItemTotal').text('-');
            $('#clusterIncTotal').text('-');
            return;
        }
        $('#clusterTpsTotal').text(formatDash(Math.floor(overview.totalTps || 0)));
        $('#clusterWorkItemTotal').text(formatDash(overview.totalFullWorkItems));
        $('#clusterIncTotal').text(formatDash(overview.totalIncremental));
    }

    function loadNodeMetrics(refreshTable) {
        doGetter('/cluster/nodes/metrics', {}, function (res) {
            if (res.success !== true) {
                if (refreshTable) {
                    bootGrowl(res.message || '加载节点指标失败', 'warning');
                }
                return;
            }
            metricsByNodeId = {};
            var overview = res.data || {};
            var nodes = overview.nodes || [];
            nodes.forEach(function (item) {
                if (item && item.nodeId) {
                    metricsByNodeId[item.nodeId] = item;
                }
            });
            if (clusterEnabled) {
                refreshChartNodeSelect(nodes);
                loadCharts();
            }
            updateTaskSummary(overview);
            if (pagination && typeof pagination.doSearch === 'function') {
                pagination.doSearch({}, pagination.currentPage || 1);
            }
        });
    }

    window.initClusterList = initClusterList;
})(window);
