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
            } else if (action === 'delete') {
                removeNode(id, $btn.attr('data-name') || id);
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
        // 本机直采可能未带 reachable；local=true 时仍应展示
        if (!m || (!m.reachable && !m.local)) {
            return '-';
        }
        return getter(m);
    }

    /** status: 0-离线；1-在线 */
    function formatStatus(status) {
        if (Number(status) === 1) {
            return '<span class="badge badge-success">在线</span>';
        }
        return '<span class="badge badge-error">离线</span>';
    }

    /** role: 0-Follower；1-Leader */
    function formatRole(role) {
        if (Number(role) === 1) {
            return '<span class="badge badge-info">Leader</span>';
        }
        return '<span class="text-secondary">Follower</span>';
    }

    function resolveNodeAddress(item) {
        if (!item) {
            return null;
        }
        var ip = item.ip;
        var port = Number(item.httpPort) || 0;
        if ((!ip || port <= 0) && item.nodeId) {
            var idx = String(item.nodeId).lastIndexOf(':');
            if (idx > 0) {
                if (!ip) {
                    ip = String(item.nodeId).substring(0, idx);
                }
                if (port <= 0) {
                    port = Number(String(item.nodeId).substring(idx + 1)) || 0;
                }
            }
        }
        if (!ip || port <= 0) {
            return null;
        }
        return {ip: ip, httpPort: port};
    }

    function buildSsoConsoleUrl(item) {
        var addr = resolveNodeAddress(item);
        if (!addr) {
            return '';
        }
        var target = addr.ip + ':' + addr.httpPort;
        return '/sso/redirect?target=' + encodeURIComponent(target) + '&redirect=' + encodeURIComponent('/');
    }

    /** 悬浮展示完整地址：优先 nodeId，否则 ip:port */
    function resolveNodeEndpoint(item) {
        if (!item) {
            return '';
        }
        if (item.nodeId && String(item.nodeId).indexOf(':') > 0) {
            return String(item.nodeId);
        }
        var addr = resolveNodeAddress(item);
        return addr ? (addr.ip + ':' + addr.httpPort) : '';
    }

    /** 多行单元格：标签 + 值 */
    function stackCell(lines) {
        if (!lines || !lines.length) {
            return '-';
        }
        var html = ['<div class="flex flex-col white-space-none text-xs">'];
        lines.forEach(function (line, index) {
            var cls = index === 0 ? '' : ' class="mt-1"';
            html.push('<div' + cls + '><span class="text-tertiary">' + escapeHtml(line.label)
                + '</span> ' + line.value + '</div>');
        });
        html.push('</div>');
        return html.join('');
    }

    function renderClusterRow(item) {
        var nodeId = item.nodeId || '';
        var name = item.name || nodeId || '';
        var localMark = item.local ? ' (本机)' : '';
        var endpoint = resolveNodeEndpoint(item);
        var endpointTitle = endpoint ? ' title="' + escapeHtml(endpoint) + '"' : '';
        var m = metricOf(nodeId);
        var fullWorkItems = m ? formatDash(m.fullWorkItemCount) : '-';
        var incremental = m ? formatDash(m.incrementalCount) : '-';
        var tps = formatMetric(m, function (metric) {
            return formatDash(Math.floor(metric.tps || 0));
        });
        var queueUp = formatMetric(m, function (metric) {
            return formatDash(metric.queueUp);
        });
        var storageQueueUp = formatMetric(m, function (metric) {
            return formatDash(metric.storageQueueUp);
        });
        var cpu = (m && (m.reachable || m.local)) ? formatPercent(m.cpuPercent) : '-';
        var memory = (m && (m.reachable || m.local)) ? formatUsedTotal(m.memoryUsed, m.memoryTotal, 'G') : '-';
        var threads = (m && (m.reachable || m.local)) ? formatDash(m.threadLive) : '-';
        var disk = (m && (m.reachable || m.local)) ? formatUsedTotal(m.diskUsed, m.diskTotal, 'G') : '-';
        var buttons = [];
        if (clusterEnabled) {
            var editId = escapeHtml(nodeId);
            buttons.push(
                '<button type="button" class="table-action-btn view" title="编辑名称" data-id="'
                + editId + '" data-action="edit"><i class="fa fa-pencil"></i></button>'
            );
            // 仅离线节点可删（status: 0-离线；1-在线）
            if (Number(item.status) === 0 && !item.local) {
                buttons.push(
                    '<button type="button" class="table-action-btn delete" title="删除节点" data-id="'
                    + editId + '" data-name="' + escapeHtml(name) + '" data-action="delete">'
                    + '<i class="fa fa-trash"></i></button>'
                );
            }
        }
        var actions = buttons.length > 0
            ? '<div class="flex items-center">' + buttons.join('') + '</div>'
            : '-';
        var nameText = escapeHtml(name) + localMark;
        var nameHtml;
        if (clusterEnabled && !item.local) {
            var consoleUrl = buildSsoConsoleUrl(item);
            if (consoleUrl) {
                nameHtml = '<a class="text-primary hover-underline"' + endpointTitle
                    + ' href="' + consoleUrl + '">' + nameText + '</a>';
            } else {
                nameHtml = '<span' + endpointTitle + '>' + nameText + '</span>';
            }
        } else {
            nameHtml = '<span' + endpointTitle + '>' + nameText + '</span>';
        }
        var statusHtml = '<div class="flex flex-col white-space-none">'
            + '<div>' + formatRole(item.role) + '</div>'
            + '<div class="mt-1">' + formatStatus(item.status) + '</div>'
            + '</div>';
        var timeHtml = stackCell([
            {label: '心跳', value: formatRelativeTime(item.heartbeatTime)},
            {label: '启动', value: formatDate(item.startTime)}
        ]);
        var taskHtml = stackCell([
            {label: '全量分片', value: escapeHtml(fullWorkItems)},
            {label: '增量任务', value: escapeHtml(incremental)}
        ]);
        var throughputHtml = stackCell([
            {label: 'TPS', value: escapeHtml(tps)},
            {label: '堆积', value: escapeHtml(queueUp)},
            {label: '持久化', value: escapeHtml(storageQueueUp)}
        ]);
        var resourceHtml = stackCell([
            {label: 'CPU', value: escapeHtml(cpu)},
            {label: '内存', value: escapeHtml(memory)},
            {label: '线程', value: escapeHtml(threads)},
            {label: '磁盘', value: escapeHtml(disk)}
        ]);
        return '<tr>'
            + '<td>' + nameHtml + '</td>'
            + '<td>' + statusHtml + '</td>'
            + '<td>' + timeHtml + '</td>'
            + '<td>' + taskHtml + '</td>'
            + '<td>' + throughputHtml + '</td>'
            + '<td>' + resourceHtml + '</td>'
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

    function removeNode(id, displayName) {
        if (!id) {
            return;
        }
        var tipName = displayName || id;
        showConfirm({
            title: '确定要删除节点？',
            icon: 'warning',
            confirmType: 'danger',
            confirmText: '删除',
            body: '<p class="mb-0">将移除离线节点 <strong>' + escapeHtml(tipName)
                + '</strong>，此操作不可恢复。</p>',
            onConfirm: function () {
                doPoster('/cluster/remove', {id: id}, function (res) {
                    if (res.success === true) {
                        bootGrowl(res.data || '删除成功', 'success');
                        loadNodeMetrics(false);
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
