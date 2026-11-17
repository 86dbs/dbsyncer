/**
 * 采集任务列表（本地 / 远端独立页面与权限）
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

    function resolvePageMeta() {
        var $root = $('.form-container[data-capture-type]').first();
        var type = $root.attr('data-capture-type') || 'captureLocal';
        var editPermission = $root.attr('data-edit-permission')
            || (type === 'captureRemote' ? 'captureRemote:edit' : 'captureLocal:edit');
        var pageBase = $root.attr('data-page-base')
            || (type === 'captureRemote' ? '/capture/remote' : '/capture/local');
        return {
            type: type,
            editPermission: editPermission,
            pageBase: pageBase,
            listUrl: pageBase + '/list'
        };
    }

    /**
     * 与 CommonTaskStatusEnum / 其它任务列表对齐：未运行、运行中、停止中。
     */
    function getTaskStateConfig(taskId) {
        return {
            0: {
                icon: 'fa-play',
                title: '启动',
                onclick: "startCaptureTask('" + taskId + "')",
                disabled: false,
                class: 'badge-info',
                text: '未运行'
            },
            1: {
                icon: 'fa-pause',
                title: '停止',
                onclick: "stopCaptureTask('" + taskId + "')",
                disabled: false,
                class: 'badge-success',
                text: '运行中'
            },
            2: {
                icon: 'fa-spinner fa-spin',
                title: '停止中',
                text: '停止中',
                disabled: true,
                class: 'badge-warning'
            }
        };
    }

    function renderTaskStateText(state) {
        var stateConfig = getTaskStateConfig('');
        var config = stateConfig[Number(state)] || stateConfig[0];
        return '<span class="badge ' + config.class + '">' + config.text + '</span>';
    }

    function renderTaskStateButton(state, taskId) {
        var stateConfig = getTaskStateConfig(taskId);
        var config = stateConfig[Number(state)] || stateConfig[0];
        var disabledAttr = config.disabled ? ' disabled' : '';
        var onclickAttr = config.onclick ? ' onclick="' + config.onclick + '"' : '';
        return '<button class="table-action-btn play" title="' + config.title + '"' + onclickAttr + disabledAttr + '>'
            + '<i class="fa ' + config.icon + '"></i></button>';
    }

    var pageMeta = resolvePageMeta();
    var captureType = pageMeta.type;
    var pagination = null;

    function renderActions(task) {
        var canEdit = !window.DBSPermission || DBSPermission.has(pageMeta.editPermission);
        var state = Number(task.state) || 0;
        var running = state === 1 || state === 2;
        var editBtn = canEdit ? `
            <button class="table-action-btn view" title="修改" onclick="doLoader('${pageMeta.pageBase}/page/edit?id=${escapeHtml(task.id)}')">
                <i class="fa fa-edit"></i>
            </button>` : '';
        var stateBtn = canEdit ? renderTaskStateButton(state, escapeHtml(task.id)) : '';
        var deleteBtn = canEdit && !running ? `
            <button class="table-action-btn delete" title="删除" onclick="deleteCaptureTask('${escapeHtml(task.id)}')">
                <i class="fa fa-trash"></i>
            </button>` : '';
        return `
            <div class="flex items-center">
                ${editBtn}
                ${stateBtn}
                ${deleteBtn}
            </div>`;
    }

    function displayText(value) {
        if (value === null || value === undefined || value === '') {
            return '-';
        }
        return escapeHtml(value);
    }

    function renderLocalActions(task) {
        var canEdit = !window.DBSPermission || DBSPermission.has(pageMeta.editPermission);
        var state = Number(task.state) || 0;
        var running = state === 1 || state === 2;
        var clientsBtn = `
            <button class="table-action-btn view js-view-local-clients" title="查看客户端"
                    data-task-id="${escapeHtml(task.id)}" data-task-name="${escapeHtml(task.name || '')}">
                <i class="fa fa-desktop"></i>
            </button>`;
        var editBtn = canEdit ? `
            <button class="table-action-btn view" title="修改" onclick="doLoader('${pageMeta.pageBase}/page/edit?id=${escapeHtml(task.id)}')">
                <i class="fa fa-edit"></i>
            </button>` : '';
        var stateBtn = canEdit ? renderTaskStateButton(state, escapeHtml(task.id)) : '';
        var deleteBtn = canEdit && !running ? `
            <button class="table-action-btn delete" title="删除" onclick="deleteCaptureTask('${escapeHtml(task.id)}')">
                <i class="fa fa-trash"></i>
            </button>` : '';
        return `
            <div class="flex items-center">
                ${clientsBtn}
                ${editBtn}
                ${stateBtn}
                ${deleteBtn}
            </div>`;
    }

    function renderLocalRow(task, index) {
        return `
            <tr>
                <td>${index}</td>
                <td>${displayText(task.name)}</td>
                <td>${renderTaskStateText(task.state)}</td>
                <td>${displayText(task.queueCapacityText)}</td>
                <td title="${displayText(task.checkpointText)}">${displayText(task.checkpointText)}</td>
                <td>${displayText(task.connectorType)}</td>
                <td>${renderLocalActions(task)}</td>
            </tr>`;
    }

    function renderClientRows(clients) {
        if (!clients || !clients.length) {
            return '<tr><td colspan="6" class="text-center text-tertiary py-8">暂无连接的客户端</td></tr>';
        }
        return clients.map(function (client) {
            return `
                <tr>
                    <td>${displayText(client.host)}</td>
                    <td>${displayText(client.port)}</td>
                    <td title="${displayText(client.subscribeText)}">${displayText(client.subscribeText)}</td>
                    <td>${formatTime(client.registerTime)}</td>
                    <td>${formatTime(client.lastHeartbeatTime)}</td>
                    <td>${displayText(client.queueCapacityText)}</td>
                </tr>`;
        }).join('');
    }

    function openLocalClientsDialog(id, name) {
        doPoster('/capture/local/clients', {id: id}, function (res) {
            if (!res.success) {
                bootGrowl(res.message || '加载客户端失败', 'danger');
                return;
            }
            var clients = res.data || [];
            var body = `
                <div class="group-dialog-body">
                    <table class="table">
                        <thead>
                        <tr>
                            <th>客户端地址</th>
                            <th>端口</th>
                            <th>订阅信息</th>
                            <th>注册信息</th>
                            <th>心跳上报时间</th>
                            <th>客户端队列容量</th>
                        </tr>
                        </thead>
                        <tbody>${renderClientRows(clients)}</tbody>
                    </table>
                </div>`;
            showConfirm({
                title: '客户端列表' + (name ? ' - ' + name : ''),
                body: body,
                size: 'xlarge',
                showCancel: false,
                confirmText: '关闭',
                confirmType: 'info'
            });
        });
    }

    $('#captureTaskTableBody').off('click.viewLocalClients').on('click.viewLocalClients', '.js-view-local-clients', function () {
        var $btn = $(this);
        openLocalClientsDialog($btn.attr('data-task-id'), $btn.attr('data-task-name') || '');
    });

    function renderRemoteRow(task, index) {
        return `
            <tr>
                <td>${index}</td>
                <td>${displayText(task.name)}</td>
                <td>${displayText(task.agentAddress)}</td>
                <td title="${displayText(task.mappingNamesText)}">${displayText(task.mappingNamesText)}</td>
                <td>${displayText(task.connectorType)}</td>
                <td>${renderTaskStateText(task.state)}</td>
                <td>${displayText(task.queueCapacityText)}</td>
                <td title="${displayText(task.checkpointText)}">${displayText(task.checkpointText)}</td>
                <td title="${displayText(task.subscribeText)}">${displayText(task.subscribeText)}</td>
                <td>${formatTime(task.updateTime)}</td>
                <td>${renderActions(task)}</td>
            </tr>`;
    }

    function renderRow(task, index) {
        if (captureType === 'captureRemote') {
            return renderRemoteRow(task, index);
        }
        return renderLocalRow(task, index);
    }

    $('#addCaptureTaskBtn').off('click').on('click', function () {
        doLoader(pageMeta.pageBase + '/page/add');
    });

    function refreshCaptureTaskList() {
        if (pagination) {
            pagination.doSearch({type: captureType}, pagination.currentPage);
        }
    }

    window.startCaptureTask = function (id) {
        doPoster('/capture/start', {id: id}, function (res) {
            if (res.success) {
                refreshCaptureTaskList();
            } else {
                bootGrowl(res.message || '启动失败', 'danger');
            }
        });
    };

    window.stopCaptureTask = function (id) {
        showConfirm({
            title: '停止采集任务',
            content: '确认停止该采集任务？',
            onConfirm: function () {
                doPoster('/capture/stop', {id: id}, function (res) {
                    if (res.success) {
                        refreshCaptureTaskList();
                    } else {
                        bootGrowl(res.message || '停止失败', 'danger');
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
                doPoster('/capture/remove', {id: id}, function (res) {
                    if (res.success) {
                        refreshCaptureTaskList();
                    } else {
                        bootGrowl(res.message || '删除失败', 'danger');
                    }
                });
            }
        });
    };

    window.backIndexPage = function () {
        doLoader(pageMeta.listUrl);
    };

    pagination = new PaginationManager({
        requestUrl: '/capture/query',
        tableBodySelector: '#captureTaskTableBody',
        renderRow: renderRow,
        storageKey: 'capture-list-' + captureType,
        params: {type: captureType},
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
