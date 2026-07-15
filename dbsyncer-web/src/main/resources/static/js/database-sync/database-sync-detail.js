/**
 * 整库迁移结果详情页
 */
(function (window) {
    'use strict';

    var migrationDetailModal = null;

    function getTaskId() {
        var root = document.getElementById('databaseSyncerDetailPage');
        return root ? (root.getAttribute('data-task-id') || '') : '';
    }

    function formatSourceDb(row) {
        var db = row.sourceDatabase || '';
        var schema = row.sourceSchema || '';
        if (db && schema) {
            return escapeHtml(db) + ' / ' + escapeHtml(schema);
        }
        return escapeHtml(db || schema || '-');
    }

    function formatTargetDb(row) {
        var db = row.targetDatabase || '';
        var schema = row.targetSchema || '';
        if (db && schema) {
            return escapeHtml(db) + ' / ' + escapeHtml(schema);
        }
        return escapeHtml(db || schema || '-');
    }

    function formatFailTotal(fail) {
        var n = Number(fail) || 0;
        if (n > 0) {
            return '<span class="badge badge-error">' + n + '</span>';
        }
        return '<span class="badge badge-success">0</span>';
    }

    function formatPrimaryKey(primaryKey) {
        if (!primaryKey || typeof primaryKey !== 'object') {
            return '';
        }
        var pairs = [];
        Object.keys(primaryKey).forEach(function (name) {
            pairs.push(name + '=' + primaryKey[name]);
        });
        return pairs.join(',');
    }

    function parseMigrationDetailRows(row) {
        var typeCode = row.type || '';
        var content = row.content || '';
        var rows = [];

        if (content) {
            try {
                var detail = typeof content === 'string' ? JSON.parse(content) : content;
                if (detail && Array.isArray(detail.records)) {
                    detail.records.forEach(function (record) {
                        var key = record.identifier || formatPrimaryKey(record.primaryKey) || '-';
                        rows.push({
                            key: key,
                            error: record.error || '-'
                        });
                    });
                    return rows;
                }
            } catch (e) {
                // 兼容旧版文本格式
            }
        }

        if (typeCode === 'rowData') {
            if (!content) {
                return rows;
            }
            content.split('\n').forEach(function (line) {
                line = (line || '').trim();
                if (!line) {
                    return;
                }
                var errorIndex = line.indexOf(';error=');
                if (errorIndex >= 0) {
                    rows.push({
                        key: line.substring(0, errorIndex),
                        error: line.substring(errorIndex + 7)
                    });
                } else if (line.indexOf('error=') === 0) {
                    rows.push({ key: '-', error: line.substring(6) });
                } else {
                    rows.push({ key: '-', error: line });
                }
            });
            return rows;
        }

        rows.push({
            key: row.sourceTable || row.targetTable || '-',
            error: content || '迁移成功'
        });
        return rows;
    }

    function buildMigrationDetailBodyHtml(row) {
        var typeCode = row.type || '';
        var isRowData = typeCode === 'rowData';
        var keyHeader = isRowData ? '主键' : '表名';
        var detailRows = parseMigrationDetailRows(row);
        var bodyHtml = '';
        if (detailRows.length === 0) {
            bodyHtml = '<tr><td colspan="2" class="text-center text-gray-400">暂无错误详情</td></tr>';
        } else {
            detailRows.forEach(function (item) {
                var errorText = item.error || '-';
                bodyHtml += '<tr>'
                    + '<td class="min-w-300">' + escapeHtml(item.key || '-') + '</td>'
                    + '<td><div class="table-cell-scroll text-sm">'
                    + escapeHtml(errorText) + '</div></td>'
                    + '</tr>';
            });
        }
        return '<table class="table">'
            + '<thead><tr><th>' + escapeHtml(keyHeader) + '</th><th class="width60p">错误信息</th></tr></thead>'
            + '<tbody>' + bodyHtml + '</tbody>'
            + '</table>';
    }

    function closeMigrationDetailModal() {
        if (migrationDetailModal) {
            migrationDetailModal.close();
            migrationDetailModal = null;
        }
    }

    function showMigrationDetail(detailId, migrationDetailCache) {
        var row = migrationDetailCache[detailId];
        if (!row) {
            bootGrowl('未找到详情数据', 'warning');
            return;
        }

        var typeCode = row.type || '';
        var isRowData = typeCode === 'rowData';
        closeMigrationDetailModal();
        migrationDetailModal = showConfirm({
            title: isRowData ? '数据迁移详情' : '结构迁移详情',
            icon: 'info',
            size: 'max',
            position: 'center',
            body: buildMigrationDetailBodyHtml(row),
            confirmText: '关闭',
            showCancel: false,
            onConfirm: function () {
                migrationDetailModal = null;
            }
        });
    }

    function initDatabaseSyncerDetailPage() {
        window.backIndexPage = function () {
            doLoader('/database-sync/list');
        };

        var currentTaskId = getTaskId();
        var migrationDetailCache = {};

        $('#detailTaskSelect').dbSelect({
            type: 'single',
            onSelect: function (selected) {
                var selectedId = selected.value || selected;
                if (selectedId && selectedId !== currentTaskId) {
                    currentTaskId = selectedId;
                    doLoader('/database-sync/page/detail?id=' + currentTaskId);
                }
            }
        });

        function getFilterValue(selector) {
            var api = $(selector).data('dbSelect');
            if (api && typeof api.getValues === 'function' && api.getValues().length > 0) {
                return api.getValues()[0] || '';
            }
            return $(selector).val() || '';
        }

        function buildSearchParams() {
            return {
                taskId: currentTaskId,
                detailType: getFilterValue('#detailTypeFilter'),
                detailStatus: getFilterValue('#detailStatusFilter')
            };
        }

        function triggerSearch() {
            resultPagination.doSearch(buildSearchParams(), 1);
        }

        function initFilterSelect(selector) {
            $(selector).dbSelect({
                type: 'single',
                onSelect: function () {
                    triggerSearch();
                }
            });
        }

        initFilterSelect('#detailTypeFilter');
        initFilterSelect('#detailStatusFilter');

        var typeTextMap = {
            rowData: '数据迁移',
            tableSchema: '表结构'
        };
        var typeBadgeMap = {
            rowData: 'badge-primary',
            tableSchema: 'badge-info'
        };

        window.showMigrationDetail = function (detailId) {
            showMigrationDetail(detailId, migrationDetailCache);
        };

        var resultPagination = new PaginationManager({
            requestUrl: '/database-sync/searchResult',
            tableBodySelector: '#detail-result-table',
            params: buildSearchParams(),
            pageSize: 10,
            customPageSize: true,
            customPageSizeItems: [5, 10, 20, 50, 100, 200],
            storageKey: 'database-sync-detail',
            renderRow: function (row, index) {
                var typeCode = row.type || '';
                var badgeClass = typeBadgeMap[typeCode] || 'badge-info';
                var typeText = typeTextMap[typeCode] || typeCode;
                var sourceTotal = row.sourceTotal;
                if (sourceTotal === undefined || sourceTotal === null || sourceTotal === '') {
                    sourceTotal = typeCode === 'rowData' ? '-' : '<span class="text-gray-400">-</span>';
                }
                var detailId = row.id != null ? String(row.id) : ('detail_' + index);
                migrationDetailCache[detailId] = {
                    type: typeCode,
                    sourceTable: row.sourceTable || '',
                    targetTable: row.targetTable || '',
                    content: row.content || ''
                };
                return '<tr>'
                    + '<td>' + index + '</td>'
                    + '<td><span class="badge ' + badgeClass + '">' + escapeHtml(typeText) + '</span></td>'
                    + '<td>' + formatSourceDb(row) + '</td>'
                    + '<td>' + escapeHtml(row.sourceTable || '-') + '</td>'
                    + '<td>' + formatTargetDb(row) + '</td>'
                    + '<td>' + escapeHtml(row.targetTable || '-') + '</td>'
                    + '<td>' + sourceTotal + '</td>'
                    + '<td>' + (row.successTotal != null ? row.successTotal : '-') + '</td>'
                    + '<td>' + formatFailTotal(row.failTotal) + '</td>'
                    + '<td>' + formatDate(row.updateTime || '') + '</td>'
                    + '<td><button type="button" class="table-action-btn view diff-detail-btn" title="查看详情" onclick="showMigrationDetail(\'' + detailId.replace(/'/g, '') + '\')"><i class="fa fa-eye"></i></button></td>'
                    + '</tr>';
            },
            emptyHtml: '<td colspan="11" class="text-center"><i class="fa fa-database empty-icon"></i>'
                + '<p class="empty-text">暂无迁移结果</p>'
                + '<p class="empty-description">任务执行完成后将在此展示各表迁移明细</p></td>'
        });
    }

    $(document).ready(initDatabaseSyncerDetailPage);
})(window);
