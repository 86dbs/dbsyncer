/**
 * 采集任务列表
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

    function typeText(type) {
        if (type === 'captureRemote') {
            return '远端采集';
        }
        return '本地采集';
    }

    function stateBadge(state) {
        if (Number(state) === 1) {
            return '<span class="badge badge-success">运行</span>';
        }
        if (Number(state) === 2) {
            return '<span class="badge badge-warning">停止中</span>';
        }
        return '<span class="badge badge-error">停止</span>';
    }

    function renderRow(task, index) {
        var canEdit = !window.DBSPermission || DBSPermission.has('capture:edit');
        var running = Number(task.state) === 1 || Number(task.state) === 2;
        var editBtn = canEdit && !running ? `
            <button class="table-action-btn view" title="修改" onclick="doLoader('/capture-task/page/edit?id=${escapeHtml(task.id)}')">
                <i class="fa fa-edit"></i>
            </button>` : '';
        var startBtn = canEdit && !running ? `
            <button class="table-action-btn play" title="启动" onclick="startCaptureTask('${escapeHtml(task.id)}')">
                <i class="fa fa-play"></i>
            </button>` : '';
        var stopBtn = canEdit && running ? `
            <button class="table-action-btn delete" title="停止" onclick="stopCaptureTask('${escapeHtml(task.id)}')">
                <i class="fa fa-stop"></i>
            </button>` : '';
        var deleteBtn = canEdit && !running ? `
            <button class="table-action-btn delete" title="删除" onclick="deleteCaptureTask('${escapeHtml(task.id)}')">
                <i class="fa fa-trash"></i>
            </button>` : '';
        return `
            <tr>
                <td>${index}</td>
                <td>${escapeHtml(task.name)}</td>
                <td>${typeText(task.type)}</td>
                <td>${stateBadge(task.state)}</td>
                <td>${formatTime(task.updateTime)}</td>
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

    var pagination = null;

    function refreshCaptureTaskList() {
        if (pagination) {
            pagination.doSearch({}, pagination.currentPage);
        }
    }

    window.startCaptureTask = function (id) {
        showConfirm({
            title: '启动采集任务',
            content: '确认启动该采集任务？',
            onConfirm: function () {
                doPoster('/capture-task/start', {id: id}, function (res) {
                    if (res.success) {
                        refreshCaptureTaskList();
                    } else {
                        alert(res.message || '启动失败');
                    }
                });
            }
        });
    };

    window.stopCaptureTask = function (id) {
        showConfirm({
            title: '停止采集任务',
            content: '确认停止该采集任务？',
            onConfirm: function () {
                doPoster('/capture-task/stop', {id: id}, function (res) {
                    if (res.success) {
                        refreshCaptureTaskList();
                    } else {
                        alert(res.message || '停止失败');
                    }
                });
            }
        });
    };

    window.deleteCaptureTask = function (id) {
        showConfirm({
            title: '删除采集任务',
            content: '确认删除该采集任务？删除后不可恢复。',
            onConfirm: function () {
                doPoster('/capture-task/remove', {id: id}, function (res) {
                    if (res.success) {
                        refreshCaptureTaskList();
                    } else {
                        alert(res.message || '删除失败');
                    }
                });
            }
        });
    };

    window.backIndexPage = function () {
        doLoader('/capture-task/list');
    };

    pagination = new PaginationManager({
        requestUrl: '/capture-task/query',
        tableBodySelector: '#captureTaskTableBody',
        renderRow: renderRow,
        storageKey: 'capture-task-list',
        refreshCompleted: function () {
            if (window.DBSPermission) {
                DBSPermission.apply('#mainContent');
            }
        }
    });

    if (typeof PageRefreshManager !== 'undefined' && PageRefreshManager.register) {
        PageRefreshManager.register(refreshCaptureTaskList);
    }
})(window);
