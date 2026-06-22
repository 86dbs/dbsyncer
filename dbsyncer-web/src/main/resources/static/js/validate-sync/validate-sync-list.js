/**
 * 订正校验列表页
 */
(function (window) {
    'use strict';

    function copyTask(id) {
        doPoster('/validate-sync/copy', { id: id }, function (response) {
            if (response.success) {
                bootGrowl('复制成功', 'success');
                backIndexPage();
            } else {
                bootGrowl('复制失败: ' + response.message, 'danger');
            }
        });
    }

    function changeTaskState(id, url, title) {
        const btn = event.target.closest('.table-action-btn');
        const originalText = btn.innerHTML;
        btn.innerHTML = '<i class="fa fa-spinner fa-spin" title="' + title + '中"></i>';
        btn.disabled = true;
        doPoster(url, { id: id }, function (response) {
            btn.innerHTML = originalText;
            btn.disabled = false;
            if (response.success) {
                bootGrowl(title + '成功', 'success');
                refreshIndexList();
            } else {
                bootGrowl(title + '失败: ' + response.message, 'danger');
            }
        });
    }

    function deleteTask(id) {
        showConfirm({
            title: '确定要删除任务？',
            icon: 'warning',
            size: 'large',
            confirmType: 'danger',
            onConfirm: function () {
                doPoster('/validate-sync/remove', { id: id }, function (response) {
                    if (response.success === true) {
                        bootGrowl('删除成功！', 'success');
                        const rawStr = localStorage.getItem('dbsyncer.pagination.validate-sync-list');
                        const raw = JSON.parse(rawStr);
                        raw.pageNum = 1;
                        localStorage.setItem('dbsyncer.pagination.validate-sync-list', JSON.stringify(raw));
                        backIndexPage();
                    } else {
                        bootGrowl(response.message || '删除失败', 'danger');
                    }
                });
            }
        });
    }

    function getTaskStateConfig(taskId) {
        return {
            0: {
                icon: 'fa-play',
                title: '启动',
                onclick: "changeTaskState('" + taskId + "', '/validate-sync/start', '启动')",
                disabled: false,
                class: 'badge-info',
                text: '未运行'
            },
            1: {
                icon: 'fa-pause',
                title: '停止',
                onclick: "changeTaskState('" + taskId + "', '/validate-sync/stop', '停止')",
                disabled: false,
                class: 'badge-success',
                text: '运行中'
            },
            2: {
                icon: 'fa-spinner fa-spin',
                title: '停止中',
                text: ' 停止中',
                disabled: true,
                class: 'badge-warning'
            }
        };
    }

    function renderTaskTriggerText(type) {
        const modelState = {
            timing: { class: 'badge-primary', text: '定时' },
            once: { class: 'badge-info', text: '执行一次' }
        };
        const config = modelState[type];
        return '<span class="badge ' + config.class + '">' + config.text + '</span>';
    }

    function renderTaskStateText(state) {
        const stateConfig = getTaskStateConfig();
        const config = stateConfig[state] || stateConfig[0];
        return '<span class="badge ' + config.class + '">' + config.text + '</span>';
    }

    function renderTaskStateButton(state, mappingId) {
        const stateConfig = getTaskStateConfig(mappingId);
        const config = stateConfig[state] || stateConfig[0];
        const disabledAttr = config.disabled ? ' disabled' : '';
        const onclickAttr = config.onclick ? ' onclick="' + config.onclick + '"' : '';
        const stateBtn = [];
        stateBtn.push(
            '<button class="table-action-btn play" data-id="' + mappingId + '" title="' + config.title + '"' + onclickAttr + disabledAttr + '>'
            + '<i class="fa ' + config.icon + '"></i></button>'
        );
        if (state === 0) {
            stateBtn.push(
                '<button class="table-action-btn delete" title="删除" onclick="deleteTask(\'' + mappingId + '\')">'
                + '<i class="fa fa-trash"></i></button>'
            );
        }
        return stateBtn.join('');
    }

    function renderTableProgressText(task) {
        var total = Number(task.totalTableCount);
        var completed = Number(task.completedTableCount);
        if (isNaN(total) || total <= 0) {
            return '';
        }
        if (isNaN(completed) || completed < 0) {
            completed = 0;
        }
        if (completed > total) {
            completed = total;
        }
        return '<span class="text-xs text-secondary whitespace-nowrap">' + completed + '/' + total + '张表</span>';
    }

    function renderTaskDurationText(task) {
        var isRunning = Number(task.status) === 1;
        if (isRunning) {
            return '';
        }
        var durationText = formatElapsedDuration(task.beginTime, task.endTime);
        if (!durationText) {
            return '';
        }
        return '<span class="text-xs text-tertiary whitespace-nowrap">任务耗时：' + durationText + '</span>';
    }

    function renderResultColumn(task) {
        var n = Number(task.errorCount);
        if (isNaN(n) || n < 0) {
            n = 0;
        }
        var taskId = String(task.id || '').replace(/'/g, '');
        var isRunning = Number(task.status) === 1;
        var progressRaw = task.progress;
        if (progressRaw === null || progressRaw === undefined || progressRaw === '') {
            if (isRunning) {
                progressRaw = 0;
            } else {
                return '';
            }
        }
        var progress = Number(progressRaw);
        if (isNaN(progress) || progress < 0) {
            progress = 0;
        }
        if (progress > 100) {
            progress = 100;
        }
        if (progress === 0 && !isRunning) {
            return '';
        }

        var state = 'success';
        if (isRunning) {
            if (progress >= 80) {
                state = 'danger';
            } else if (progress >= 60) {
                state = 'warning';
            }
        } else if (n > 0) {
            state = 'danger';
        }

        var left = (n > 0)
            ? ('异常：<a href="javascript:void(0)" class="hover:underline cursor-pointer" title="查看异常校验结果" '
                + 'onclick="doLoader(\'/validate-sync/page/detail?id=' + taskId + '&detailStatus=fail\'); return false;">'
                + '<span class="badge badge-error">' + n + '</span></a>')
            : '<span class="badge badge-success">正常</span>';
        var tableProgressHtml = renderTableProgressText(task);
        var durationHtml = renderTaskDurationText(task);
        var centerParts = [];
        if (tableProgressHtml) {
            centerParts.push(tableProgressHtml);
        }
        if (durationHtml) {
            centerParts.push(durationHtml);
        }
        var centerHtml = centerParts.length
            ? ('<span class="progress-meta">' + centerParts.join('') + '</span>')
            : '';
        var rightText = (isRunning && progress === 0) ? '0%' : (progress + '%');
        return ''
            + '<div class="min-w-200">'
            + '  <div class="progress-header progress-header-compact">'
            + '    <span class="progress-title">' + left + '</span>'
            + centerHtml
            + '    <span class="progress-value progress-value-' + state + '">' + rightText + '</span>'
            + '  </div>'
            + '  <div class="progress-bar progress-bar-compact">'
            + '    <div class="progress-fill ' + state + '" data-progress-width="' + progress + '"></div>'
            + '  </div>'
            + '</div>';
    }

    function renderConnectorCell(task) {
        var sourceType = escapeHtml(task.sourceConnector.config.connectorType || '');
        var targetType = escapeHtml(task.targetConnector.config.connectorType || '');
        return ''
            + '<div class="flex items-center">'
            + '<img draggable="false" class="connector-logo mr-3" src="' + $basePath + '/img/' + sourceType + '.png" alt="">'
            + '<span>' + escapeHtml(task.sourceConnector.name || '') + '</span>'
            + '</div>'
            + '<i class="fa fa-angle-double-down ml-3"></i>'
            + '<div class="flex items-center">'
            + '<img draggable="false" class="connector-logo mr-3" src="' + $basePath + '/img/' + targetType + '.png" alt="">'
            + '<span>' + escapeHtml(task.targetConnector.name || '') + '</span>'
            + '</div>';
    }

    function initValidateSyncListPage() {
        window.backIndexPage = function () {
            doLoader('/validate-sync/list');
        };
        window.copyTask = copyTask;
        window.changeTaskState = changeTaskState;
        window.deleteTask = deleteTask;

        const pagination = new PaginationManager({
            requestUrl: '/validate-sync/search',
            tableBodySelector: '#validate-sync-table',
            storageKey: 'validate-sync-list',
            pageSize: 20,
            renderRow: function (task, index) {
                const taskId = String(task.id).replace(/"/g, '');
                return ''
                    + '<tr>'
                    + '<td>' + index + '</td>'
                    + '<td>'
                    + '<a href="javascript:void(0)" class="text-primary hover:underline cursor-pointer" title="点击查看详情" '
                    + 'onclick="doLoader(\'/validate-sync/page/detail?id=' + taskId + '\')">' + escapeHtml(task.name) + '</a>'
                    + '</td>'
                    + '<td title="' + escapeHtml(task.sourceConnector.name || '') + ' > ' + escapeHtml(task.targetConnector.name || '') + '">'
                    + renderConnectorCell(task)
                    + '</td>'
                    + '<td>' + renderTaskTriggerText(task.trigger) + '</td>'
                    + '<td>' + renderResultColumn(task) + '</td>'
                    + '<td>' + renderTaskStateText(task.status || 0) + '</td>'
                    + '<td>' + formatDate(task.updateTime || '') + '</td>'
                    + '<td><div class="flex items-center">'
                    + '<button class="table-action-btn view" title="修改" onclick="doLoader(\'/validate-sync/page/edit?id=' + task.id + '\')">'
                    + '<i class="fa fa-edit"></i></button>'
                    + renderTaskStateButton(task.status || 0, task.id)
                    + '</div></td>'
                    + '</tr>';
            },
            emptyHtml: '<td colspan="9" class="text-center"><i class="fa fa-exchange empty-icon"></i>'
                + '<p class="empty-text">暂无任务</p><p class="empty-description">点击"添加"按钮创建第一个任务</p></td>',
            customPageSize: true
        });

        const searchInput = initSearch('validate-sync-search', function (searchKey) {
            pagination.doSearch({ searchKey: searchKey }, 1);
        });

        window.refreshIndexList = function () {
            pagination.doSearch({ searchKey: searchInput.getValue() }, pagination.currentPage);
        };

        PageRefreshManager.register(function () {
            pagination.doSearch({ searchKey: searchInput.getValue() }, pagination.currentPage);
        });
    }

    $(document).ready(initValidateSyncListPage);
})(window);
