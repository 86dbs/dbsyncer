/**
 * 整库迁移列表页
 */
(function (window) {
    'use strict';

    /** 与 CommonTaskStatusEnum.DONE 对齐：本轮业务已完成 */
    var META_STATE_DONE = 3;

    function databaseSyncStart(taskId) {
        doPoster('/database-sync/start', {id: taskId}, function (response) {
            if (response.success) {
                bootGrowl(response.data || '启动成功', 'success');
                refreshIndexList();
            } else {
                bootGrowl(response.message || '启动失败', 'danger');
            }
        });
    }

    function databaseSyncStop(taskId) {
        doPoster('/database-sync/stop', {id: taskId}, function (response) {
            if (response.success) {
                bootGrowl(response.data || '停止成功', 'success');
                refreshIndexList();
            } else {
                bootGrowl(response.message || '停止失败', 'danger');
            }
        });
    }

    function databaseSyncRemove(taskId) {
        if (!confirm('确定删除该任务？')) {
            return;
        }
        doPoster('/database-sync/remove', {id: taskId}, function (response) {
            if (response.success) {
                bootGrowl(response.data || '删除成功', 'success');
                refreshIndexList();
            } else {
                bootGrowl(response.message || '删除失败', 'danger');
            }
        });
    }

    function getTaskStateConfig(taskId) {
        return {
            0: {
                icon: 'fa-play',
                title: '启动',
                onclick: "databaseSyncStart('" + taskId + "')",
                disabled: false,
                class: 'badge-info',
                text: '未运行'
            },
            1: {
                icon: 'fa-pause',
                title: '停止',
                onclick: "databaseSyncStop('" + taskId + "')",
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
            },
            3: {
                icon: 'fa-play',
                title: '启动',
                onclick: "databaseSyncStart('" + taskId + "')",
                disabled: false,
                class: 'badge-primary',
                text: '已完成'
            }
        };
    }

    function renderTaskStateText(state) {
        const stateConfig = getTaskStateConfig();
        const config = stateConfig[state] || stateConfig[0];
        return '<span class="badge ' + config.class + '">' + config.text + '</span>';
    }

    function renderTaskStateButton(state, taskId) {
        const stateConfig = getTaskStateConfig(taskId);
        const config = stateConfig[state] || stateConfig[0];
        const disabledAttr = config.disabled ? ' disabled' : '';
        const onclickAttr = config.onclick ? ' onclick="' + config.onclick + '"' : '';
        let html = '<button class="table-action-btn play" title="' + config.title + '"' + onclickAttr + disabledAttr + '>'
            + '<i class="fa ' + config.icon + '"></i></button>';
        if (state === 0 || state === 3) {
            html += '<button class="table-action-btn delete" title="删除" onclick="databaseSyncRemove(\'' + taskId + '\')">'
                + '<i class="fa fa-trash"></i></button>';
        }
        return html;
    }

    function formatCount(n) {
        if (n === null || n === undefined || isNaN(n)) {
            return '0';
        }
        try {
            return Number(n).toLocaleString('zh-CN');
        } catch (e) {
            return String(n);
        }
    }

    /**
     * 按组合模式与阶段展示：结构 a/b 或 数据 c/d行（分母未齐为「统计中」）。
     */
    function renderProgressMetaText(task) {
        var isRunning = Number(task.metaState) === 1;
        var tableTotal = Number(task.totalTableCount);
        var schemaDone = Number(task.schemaCompletedCount);
        var completed = Number(task.completedTableCount);
        var synced = Number(task.syncedRows);
        var sourceTotal = Number(task.sourceTotal);
        if (isNaN(tableTotal) || tableTotal < 0) {
            tableTotal = 0;
        }
        if (isNaN(schemaDone) || schemaDone < 0) {
            schemaDone = 0;
        }
        if (isNaN(completed) || completed < 0) {
            completed = 0;
        }
        if (isNaN(synced) || synced < 0) {
            synced = 0;
        }
        if (isNaN(sourceTotal) || sourceTotal < 0) {
            sourceTotal = 0;
        }
        if (schemaDone > tableTotal && tableTotal > 0) {
            schemaDone = tableTotal;
        }
        if (completed > tableTotal && tableTotal > 0) {
            completed = tableTotal;
        }
        if (sourceTotal > 0 && synced > sourceTotal) {
            synced = sourceTotal;
        }

        var enableSchema = !!task.enableCopySchema;
        var enableData = !!task.enableCopyData;

        if (!isRunning) {
            if (enableData && sourceTotal > 0) {
                return '<span class="text-xs text-secondary whitespace-nowrap" title="已同步行 / 源端总行">'
                    + formatCount(synced) + '/' + formatCount(sourceTotal) + '行</span>';
            }
            if (tableTotal > 0) {
                return '<span class="text-xs text-secondary whitespace-nowrap" title="已完成表 / 总表">'
                    + completed + '/' + tableTotal + '张表</span>';
            }
            return '';
        }

        if (enableSchema && !enableData) {
            if (tableTotal <= 0) {
                return '';
            }
            return '<span class="text-xs text-secondary whitespace-nowrap" title="结构完成表 / 总表">'
                + '结构 ' + schemaDone + '/' + tableTotal + '</span>';
        }

        if (enableSchema && enableData && schemaDone < tableTotal) {
            if (tableTotal <= 0) {
                return '';
            }
            return '<span class="text-xs text-secondary whitespace-nowrap" title="结构完成表 / 总表">'
                + '结构 ' + schemaDone + '/' + tableTotal + '</span>';
        }

        if (enableData) {
            var totalText = sourceTotal > 0 ? formatCount(sourceTotal) : '统计中';
            return '<span class="text-xs text-secondary whitespace-nowrap" title="已同步行 / 源端总行">'
                + '数据 ' + formatCount(synced) + '/' + totalText + '</span>';
        }

        return '';
    }

    function renderTaskDurationText(task) {
        var isRunning = Number(task.metaState) === 1;
        if (isRunning) {
            return '';
        }
        var durationText = formatElapsedDuration(task.startTime, task.updateTime);
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
        var isRunning = Number(task.metaState) === 1;
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
        } else if (Number(task.metaState) !== META_STATE_DONE && !isRunning && progress > 0 && progress < 100) {
            state = 'warning';
        } else if (n > 0) {
            state = 'danger';
        }

        var left = (n > 0)
            ? ('异常：<a href="javascript:void(0)" class="hover:underline cursor-pointer" title="查看失败迁移结果" '
                + 'onclick="doLoader(\'/database-sync/page/detail?id=' + taskId + '&detailStatus=fail\'); return false;">'
                + '<span class="badge badge-error">' + n + '</span></a>')
            : '<span class="badge badge-success">正常</span>';
        var tableProgressHtml = renderProgressMetaText(task);
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

    function initDatabaseSyncerListPage() {
        window.backIndexPage = function () {
            doLoader('/database-sync/list');
        };
        window.databaseSyncStart = databaseSyncStart;
        window.databaseSyncStop = databaseSyncStop;
        window.databaseSyncRemove = databaseSyncRemove;

        const pagination = new PaginationManager({
            requestUrl: '/database-sync/search',
            tableBodySelector: '#database-sync-table',
            storageKey: 'database-sync-list',
            renderRow: function (task, index) {
                const mappingCount = task.mappingCount != null ? task.mappingCount : 0;
                const taskId = String(task.id || '').replace(/"/g, '');
                return ''
                    + '<tr>'
                    + '<td>' + index + '</td>'
                    + '<td>'
                    + '<a href="javascript:void(0)" class="text-primary hover:underline cursor-pointer" title="点击查看迁移结果" '
                    + 'onclick="doLoader(\'/database-sync/page/detail?id=' + taskId + '\')">'
                    + escapeHtml(task.name || '') + '</a>'
                    + '</td>'
                    + '<td>' + mappingCount + '</td>'
                    + '<td>' + renderResultColumn(task) + '</td>'
                    + '<td>' + renderTaskStateText(task.metaState || 0) + '</td>'
                    + '<td>' + formatDate(task.updateTime || '') + '</td>'
                    + '<td><div class="flex items-center">'
                    + '<button class="table-action-btn view" title="修改" onclick="doLoader(\'/database-sync/page/edit?id='
                    + taskId + '\')">'
                    + '<i class="fa fa-edit"></i></button>'
                    + renderTaskStateButton(task.metaState || 0, task.id)
                    + '</div></td>'
                    + '</tr>';
            },
            emptyHtml: '<td colspan="8" class="text-center">'
                + '<i class="fa fa-database empty-icon"></i>'
                + '<p class="empty-text">暂无任务</p>'
                + '<p class="empty-description">点击「添加」创建第一个任务</p></td>',
            customPageSize: true
        });

        const searchInput = initSearch('database-sync-search', function (searchKey) {
            pagination.doSearch({searchKey: searchKey}, 1);
        });

        window.refreshIndexList = function () {
            pagination.doSearch({searchKey: searchInput.getValue()}, pagination.currentPage);
        };

        PageRefreshManager.register(function () {
            pagination.doSearch({searchKey: searchInput.getValue()}, pagination.currentPage);
        });
    }

    $(document).ready(initDatabaseSyncerListPage);
})(window);
