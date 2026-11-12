/**
 * Agent 管理列表
 */
(function (window) {
    'use strict';

    function escapeHtml(text) {
        if (text === null || text === undefined) {
            return '';
        }
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function formatTime(ms) {
        if (!ms) {
            return '-';
        }
        var d = new Date(Number(ms));
        if (isNaN(d.getTime())) {
            return '-';
        }
        var pad = function (n) {
            return n < 10 ? '0' + n : '' + n;
        };
        return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
            + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
    }

    function stateBadge(state) {
        if (Number(state) === 1) {
            return '<span class="badge badge-success">运行</span>';
        }
        return '<span class="badge badge-error">停止</span>';
    }

    function renderRow(agent, index) {
        var canEdit = !window.DBSPermission || DBSPermission.has('agent:edit');
        var running = Number(agent.state) === 1;
        var editBtn = canEdit && !running ? `
            <button class="table-action-btn view" title="修改" onclick="doLoader('/agent/page/edit?id=${escapeHtml(agent.id)}')">
                <i class="fa fa-edit"></i>
            </button>` : '';
        var startBtn = canEdit && !running ? `
            <button class="table-action-btn play" title="启动" onclick="startAgent('${escapeHtml(agent.id)}')">
                <i class="fa fa-play"></i>
            </button>` : '';
        var stopBtn = canEdit && running ? `
            <button class="table-action-btn delete" title="停止" onclick="stopAgent('${escapeHtml(agent.id)}')">
                <i class="fa fa-stop"></i>
            </button>` : '';
        var deleteBtn = canEdit && !running ? `
            <button class="table-action-btn delete" title="删除" onclick="deleteAgent('${escapeHtml(agent.id)}')">
                <i class="fa fa-trash"></i>
            </button>` : '';
        return `
            <tr>
                <td>${index}</td>
                <td>${escapeHtml(agent.name)}</td>
                <td>${escapeHtml(agent.listenPort)}</td>
                <td>${stateBadge(agent.state)}</td>
                <td>${formatTime(agent.updateTime)}</td>
                <td>
                    <div class="flex items-center">
                        ${editBtn}
                        ${startBtn}
                        ${stopBtn}
                        ${deleteBtn}
                    </div>
                </td>
            </tr>`;
    }

    window.startAgent = function (id) {
        showConfirm({
            title: '启动 Agent',
            content: '确认启动该 Agent？',
            onConfirm: function () {
                doPoster('/agent/start', {id: id}, function (res) {
                    if (res.success) {
                        pagination.refresh();
                    } else {
                        alert(res.message || '启动失败');
                    }
                });
            }
        });
    };

    window.stopAgent = function (id) {
        showConfirm({
            title: '停止 Agent',
            content: '确认停止该 Agent？',
            onConfirm: function () {
                doPoster('/agent/stop', {id: id}, function (res) {
                    if (res.success) {
                        pagination.refresh();
                    } else {
                        alert(res.message || '停止失败');
                    }
                });
            }
        });
    };

    window.deleteAgent = function (id) {
        showConfirm({
            title: '删除 Agent',
            content: '确认删除该 Agent？删除后不可恢复。',
            onConfirm: function () {
                doPoster('/agent/remove', {id: id}, function (res) {
                    if (res.success) {
                        pagination.refresh();
                    } else {
                        alert(res.message || '删除失败');
                    }
                });
            }
        });
    };

    var pagination = null;

    window.backIndexPage = function () {
        doLoader('/agent/list');
    };

    pagination = new PaginationManager({
        requestUrl: '/agent/query',
        tableBodySelector: '#agentTableBody',
        renderRow: renderRow,
        storageKey: 'agent-list',
        refreshCompleted: function () {
            if (window.DBSPermission) {
                DBSPermission.apply('#mainContent');
            }
        }
    });
})(window);
