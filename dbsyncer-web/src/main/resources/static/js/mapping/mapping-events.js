/**
 * 同步事件明细页（迁自监控 queryData）
 */
(function (window) {
    'use strict';

    function getPageAttr(name) {
        var root = document.getElementById('mappingEventsPage');
        return root ? (root.getAttribute(name) || '') : '';
    }

    function showMessageDetail($message, icon, title) {
        $message.unbind('click').bind('click', function () {
            var content = $(this).text();
            if (content && content.trim()) {
                showConfirm({
                    title: title,
                    icon: icon,
                    size: 'max',
                    message: content
                });
            }
        });
    }

    function showRetryDetail(metaId, messageId) {
        doLoader('/monitor/page/retry?metaId=' + encodeURIComponent(metaId)
            + '&messageId=' + encodeURIComponent(messageId));
    }

    function jsonToTable(jsonObj) {
        var $content = '<table class="table">';
        $content += '<thead><tr><th></th><th>字段</th><th>值</th></tr></thead>';
        $content += '<tbody>';
        var index = 1;
        $.each(jsonObj, function (name, value) {
            $content += '<tr>';
            $content += '<td>' + index + '</td>';
            $content += '<td>' + escapeHtml(name || '') + '</td>';
            $content += '<td class="white-space-none">' + escapeHtml(value || '') + '</td>';
            $content += '</tr>';
            index++;
        });
        $content += '</tbody>';
        $content += '</table>';
        return $content;
    }

    function columnsToTable(columns) {
        var $content = '<table class="table">';
        $content += '<thead><tr><th></th><th>字段</th><th>类型</th><th>值</th></tr></thead>';
        $content += '<tbody>';
        if (!columns || !columns.length) {
            $content += '<tr><td colspan="4" class="text-center text-gray-400">暂无字段数据</td></tr>';
        } else {
            columns.forEach(function (col, i) {
                var val = col.value;
                if (val === null || val === undefined) {
                    val = 'NULL';
                }
                $content += '<tr>';
                $content += '<td>' + (i + 1) + '</td>';
                $content += '<td>' + escapeHtml(col.key || '') + '</td>';
                $content += '<td>' + escapeHtml(col.keyType || '') + '</td>';
                $content += '<td class="white-space-none">' + escapeHtml(String(val)) + '</td>';
                $content += '</tr>';
            });
        }
        $content += '</tbody></table>';
        return $content;
    }

    window.showMappingDataDetail = function (metaId, messageId) {
        doPoster('/monitor/getDataDetail', { metaId: metaId, messageId: messageId }, function (response) {
            if (!response.success) {
                bootGrowl(response.message || '加载数据失败', 'danger');
                return;
            }
            var message = response.data || {};
            var body;
            if (message.columns && message.columns.length) {
                body = columnsToTable(message.columns);
            } else {
                body = jsonToTable({});
            }
            showConfirm({
                title: '数据详情',
                icon: 'info',
                size: 'max',
                body: body,
                confirmText: '关闭',
                confirmType: 'primary'
            });
        });
    };

    function renderDataState(success) {
        var state = {
            0: { class: 'badge-error', text: '失败' },
            1: { class: 'badge-success', text: '成功' }
        };
        var config = state[success] || { class: 'badge-info', text: '-' };
        return '<span class="badge ' + config.class + '">' + config.text + '</span>';
    }

    function initMappingEventsPage() {
        var mappingId = getPageAttr('data-mapping-id');
        var metaId = getPageAttr('data-meta-id');
        var tableGroupId = getPageAttr('data-table-group-id');
        var statusSelect;
        var pagination;

        window.backDetailPage = function () {
            doLoader('/mapping/page/detail?id=' + encodeURIComponent(mappingId));
        };

        function params() {
            return {
                id: metaId || '',
                status: (statusSelect && statusSelect.getValues()[0]) || '',
                tableGroupId: tableGroupId || ''
            };
        }

        function search() {
            pagination.doSearch(params());
        }

        statusSelect = $('#searchDataStatus').dbSelect({
            type: 'single',
            onSelect: search
        });

        function renderDataButton(row) {
            var content = [];
            content.push('<button class="table-action-btn view" title="查看数据" onclick="showMappingDataDetail(\''
                + metaId + '\',\'' + row.id + '\')"><i class="fa fa-eye"></i></button>');
            if (row.success === 0) {
                content.push('<button class="table-action-btn play" title="重试" onclick="showRetryDetail(\''
                    + metaId + '\',\'' + row.id + '\')"><i class="fa fa-refresh"></i></button>');
            }
            return content.join(' ');
        }

        window.showRetryDetail = showRetryDetail;

        pagination = new PaginationManager({
            requestUrl: '/monitor/queryData',
            tableBodySelector: '#dataTableBody',
            params: params(),
            pageSize: 10,
            customPageSize: true,
            customPageSizeItems: [5, 10, 20, 50, 100],
            storageKey: 'mapping-events',
            showBoundaryButtons: true,
            renderRow: function (d, index) {
                return '<tr>'
                    + '<td>' + index + '</td>'
                    + '<td>' + escapeHtml(d.targetTableName || '') + '</td>'
                    + '<td>' + escapeHtml(d.event || '') + '</td>'
                    + '<td>' + renderDataState(d.success) + '</td>'
                    + '<td><span class="hover-underline cursor-pointer data-error">'
                    + escapeHtml(d.error || '') + '</span></td>'
                    + '<td>' + formatDate(d.createTime) + '</td>'
                    + '<td><div class="flex items-center">' + renderDataButton(d) + '</div></td>'
                    + '</tr>';
            },
            refreshCompleted: function () {
                showMessageDetail($('.data-error'), 'warning', '异常信息');
            },
            emptyHtml: '<td colspan="7" class="text-center"><i class="fa fa-exchange empty-icon"></i>'
                + '<p class="empty-text">暂无数据</p></td>'
        });

        $('#clearDataBtn').unbind('click').bind('click', function () {
            showConfirm({
                title: '确认清空当前表数据？',
                icon: 'warning',
                size: 'large',
                confirmType: 'danger',
                onConfirm: function () {
                    doPoster('/monitor/clearData', {
                        id: metaId || '',
                        tableGroupId: tableGroupId || ''
                    }, function (response) {
                        if (response.success) {
                            bootGrowl(response.data || '清空数据成功!', 'success');
                            search();
                        } else {
                            bootGrowl('清空数据失败: ' + response.message, 'danger');
                        }
                    });
                }
            });
        });
    }

    $(document).ready(initMappingEventsPage);
})(window);
