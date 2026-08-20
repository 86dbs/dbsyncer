/**
 * 集群管理列表
 */
(function (window) {
    'use strict';

    var clusterEnabled = false;
    var currentIsLeader = false;
    var pagination = null;
    var metricsByNodeId = {};
    var metricsOverview = null;

    function initClusterList(options) {
        options = options || {};
        clusterEnabled = options.clusterEnabled === true;
        currentIsLeader = options.currentIsLeader === true;
        metricsByNodeId = {};
        metricsOverview = null;

        window.backIndexPage = function () {
            doLoader('/cluster/list');
        };

        bindTabs();
        bindClusterActions();

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

    function bindTabs() {
        var $tabs = $('#clusterPageTabs');
        if (!$tabs.length) {
            return;
        }
        $tabs.on('click', '.page-tab', function () {
            var tab = $(this).attr('data-tab');
            if (!tab) {
                return;
            }
            $tabs.find('.page-tab').removeClass('is-active');
            $(this).addClass('is-active');
            $('#clusterTabPanels .page-tab-panel').removeClass('is-active');
            $('#clusterTab-' + tab).addClass('is-active');
            if (tab === 'task') {
                renderTaskDetails();
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
        var cpu = m && m.reachable ? formatPercent(m.cpuPercent) : '-';
        var memory = m && m.reachable ? formatUsedTotal(m.memoryUsed, m.memoryTotal, 'G') : '-';
        var threads = m && m.reachable ? formatDash(m.threadLive) : '-';
        var disk = m && m.reachable ? formatUsedTotal(m.diskUsed, m.diskTotal, 'G') : '-';
        var buttons = [];
        if (clusterEnabled && !item.local) {
            var consoleUrl = buildSsoConsoleUrl(item);
            if (consoleUrl) {
                buttons.push(
                    '<a class="table-action-btn view" title="打开控制台" href="'
                    + consoleUrl + '"><i class="fa fa-external-link"></i></a>'
                );
            }
        }
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
        return '<tr>'
            + '<td>' + escapeHtml(name) + localMark + '</td>'
            + '<td>' + escapeHtml(item.roleName || '') + '</td>'
            + '<td>' + escapeHtml(item.statusName || '') + '</td>'
            + '<td class="' + networkClass + '">' + networkText + '</td>'
            + '<td>' + escapeHtml(item.ip || '-') + '</td>'
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
            $('#clusterShardTotal').text('-');
            $('#clusterIncTotal').text('-');
            return;
        }
        $('#clusterTpsTotal').text(formatDash(overview.totalTps));
        $('#clusterShardTotal').text(formatDash(overview.totalFullShards));
        $('#clusterIncTotal').text(formatDash(overview.totalIncremental));
    }

    function renderTaskDetails() {
        var $body = $('#taskDetailTableBody');
        if (!$body.length) {
            return;
        }
        if (!metricsOverview || !metricsOverview.nodes || metricsOverview.nodes.length === 0) {
            $body.html('<tr><td colspan="7" class="text-center text-secondary">暂无节点指标</td></tr>');
            updateTaskSummary(metricsOverview);
            return;
        }
        updateTaskSummary(metricsOverview);
        var rows = metricsOverview.nodes.map(function (item) {
            var name = item.name || item.nodeId || '';
            var tps = item.reachable ? formatDash(Math.floor(item.tps || 0)) : '-';
            var queue = item.reachable ? formatDash(item.queueUp) : '-';
            var storage = item.reachable ? formatDash(item.storageQueueUp) : '-';
            return '<tr>'
                + '<td>' + escapeHtml(name) + '</td>'
                + '<td>' + escapeHtml(item.roleName || '') + '</td>'
                + '<td>' + formatDash(item.fullShardCount) + '</td>'
                + '<td>' + formatDash(item.incrementalCount) + '</td>'
                + '<td>' + tps + '</td>'
                + '<td>' + queue + '</td>'
                + '<td>' + storage + '</td>'
                + '</tr>';
        }).join('');
        $body.html(rows);
    }

    function loadNodeMetrics(refreshTable) {
        doGetter('/cluster/nodes/metrics', {}, function (res) {
            if (res.success !== true) {
                if (refreshTable) {
                    bootGrowl(res.message || '加载节点指标失败', 'warning');
                }
                return;
            }
            metricsOverview = res.data || {};
            metricsByNodeId = {};
            var nodes = metricsOverview.nodes || [];
            nodes.forEach(function (item) {
                if (item && item.nodeId) {
                    metricsByNodeId[item.nodeId] = item;
                }
            });
            if (refreshTable && pagination && typeof pagination.doSearch === 'function') {
                pagination.doSearch({}, pagination.currentPage || 1);
            } else if (pagination && typeof pagination.doSearch === 'function') {
                // 定时刷新：仅在集群信息 Tab 可见时重绘当前页
                if ($('#clusterTab-info').hasClass('is-active')) {
                    pagination.doSearch({}, pagination.currentPage || 1);
                }
            }
            if ($('#clusterTab-task').hasClass('is-active')) {
                renderTaskDetails();
            } else {
                updateTaskSummary(metricsOverview);
            }
        });
    }

    window.initClusterList = initClusterList;
})(window);
