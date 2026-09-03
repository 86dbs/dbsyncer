/**
 * 集群管理列表
 */
(function (window) {
    'use strict';

    var charts = {
        queue: null,
        tps: null,
        workItem: null
    };

    var clusterEnabled = false;
    var pagination = null;
    var metricsByNodeId = {};

    function initClusterList(options) {
        options = options || {};
        clusterEnabled = options.clusterEnabled === true;
        metricsByNodeId = {};

        window.backIndexPage = function () {
            doLoader('/cluster/list');
        };

        bindClusterActions();
        destroyCharts();
        if (clusterEnabled) {
            initCharts();
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
        charts.queue = initLineChart('clusterQueueChart', '堆积', 'rgba(250, 173, 20, 1)', false);
        charts.tps = initLineChart('clusterTpsChart', 'TPS', 'rgba(245, 108, 108, 1)', false);
        charts.workItem = initLineChart('clusterWorkItemChart', '全量分片', 'rgba(24, 144, 255, 1)', true);
    }

    function updateLineChart(chart, labels, data) {
        if (!chart) {
            return;
        }
        chart.data.labels = labels || [];
        chart.data.datasets[0].data = data || [];
        chart.update('none');
    }

    function toNumberList(values) {
        if (!values || !values.length) {
            return [];
        }
        return values.map(function (item) {
            return Number(item) || 0;
        });
    }

    function updateClusterCharts(overview) {
        if (!clusterEnabled || !overview) {
            return;
        }
        if (overview.tps) {
            updateLineChart(charts.tps, overview.tps.name || [], toNumberList(overview.tps.value));
            var avg = Number(overview.tps.average) || 0;
            $('#clusterTpsTitle').text(avg > 0 ? ('TPS, 平均:' + Math.floor(avg) + '/秒') : 'TPS');
        }
        if (overview.queue) {
            updateLineChart(charts.queue, overview.queue.name || [], toNumberList(overview.queue.value));
        }
        if (overview.fullWorkItems) {
            updateLineChart(charts.workItem, overview.fullWorkItems.name || [],
                toNumberList(overview.fullWorkItems.value));
        }
    }

    function bindClusterActions() {
        $('#clusterTableBody').on('click', '[data-action]', function () {
            var $btn = $(this);
            var id = $btn.attr('data-id');
            var action = $btn.attr('data-action');
            if (action === 'edit') {
                editNodeName(id);
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
        var target = item.ip + ':' + item.httpPort;
        return '/sso/redirect?target=' + encodeURIComponent(target) + '&redirect=' + encodeURIComponent('/');
    }

    function renderClusterRow(item) {
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
        if (clusterEnabled) {
            var nodeId = escapeHtml(item.id || '');
            buttons.push(
                '<button type="button" class="table-action-btn view" title="编辑名称" data-id="'
                + nodeId + '" data-action="edit"><i class="fa fa-pencil"></i></button>'
            );
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
            + '<td>' + escapeHtml(item.statusName || '') + '</td>'
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

    function editNodeName(id) {
        if (!id) {
            return;
        }
        var metric = metricOf(id);
        var currentName = (metric && metric.name) ? metric.name : id;
        var inputId = 'clusterNodeNameInput';
        showConfirm({
            title: '编辑节点',
            icon: 'info',
            confirmText: '保存',
            body: '<div class="form-item mb-0">'
                + '<label class="form-label" for="' + inputId + '">节点名称</label>'
                + '<div class="form-control-area">'
                + '<input type="text" id="' + inputId + '" class="form-control" maxlength="64"/>'
                + '</div></div>',
            onConfirm: function () {
                var $input = $('#' + inputId);
                var name = ($input.length ? $input.val() : '') || '';
                if (!String(name).trim()) {
                    bootGrowl('节点名称不能为空', 'warning');
                    return;
                }
                doPoster('/cluster/edit', {id: id, name: String(name).trim()}, function (res) {
                    if (res.success === true) {
                        bootGrowl('已保存', 'success');
                        loadNodeMetrics(false);
                    } else {
                        bootGrowl(res.message || '保存失败', 'danger');
                    }
                });
            }
        });
        setTimeout(function () {
            var el = document.getElementById(inputId);
            if (el) {
                el.value = currentName;
                el.focus();
                el.select();
            }
        }, 0);
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
                updateClusterCharts(overview);
            }
            updateTaskSummary(overview);
            if (pagination && typeof pagination.doSearch === 'function') {
                pagination.doSearch({}, pagination.currentPage || 1);
            }
        });
    }

    window.initClusterList = initClusterList;
})(window);
