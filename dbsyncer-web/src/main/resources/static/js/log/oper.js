(function () {
    'use strict';

    /** 日期字符串 yyyy-MM-dd → 当天 00:00:00 毫秒 */
    function toBeginMillis(value) {
        if (!value) {
            return '';
        }
        const t = new Date(value + 'T00:00:00').getTime();
        return isNaN(t) ? '' : t;
    }

    /** 日期字符串 yyyy-MM-dd → 当天 23:59:59.999 毫秒 */
    function toEndMillis(value) {
        if (!value) {
            return '';
        }
        const t = new Date(value + 'T23:59:59.999').getTime();
        return isNaN(t) ? '' : t;
    }

    function statusBadge(status) {
        if (Number(status) === 1) {
            return '<span class="badge badge-success">成功</span>';
        }
        return '<span class="badge badge-error">失败</span>';
    }

    function formatJsonBlock(value) {
        if (value === undefined || value === null || value === '') {
            return '无';
        }
        let text;
        if (typeof value === 'string') {
            text = value;
        } else {
            try {
                text = JSON.stringify(value, null, 2);
            } catch (e) {
                text = String(value);
            }
        }
        return '<pre class="code-block log-json-block"><code>' + escapeHtml(text) + '</code></pre>';
    }

    function getStatusFilter() {
        const vals = statusSelect && typeof statusSelect.getValues === 'function'
            ? statusSelect.getValues() : [];
        const v = vals.length ? vals[0] : ($('#filterStatus').val() || '');
        return v === '-1' || v === '' ? '' : v;
    }

    function collectParams() {
        return {
            username: $('#filterUsername').val() || '',
            ip: $('#filterIp').val() || '',
            status: getStatusFilter(),
            beginTime: toBeginMillis($('#filterBeginTime').val()),
            endTime: toEndMillis($('#filterEndTime').val())
        };
    }

    function showDetail(id) {
        doPoster('/log/detail', {id: id}, function (res) {
            if (!res || res.success !== true) {
                bootGrowl((res && res.message) || '查询详情失败', 'danger');
                return;
            }
            const d = res.data || {};
            const body = ''
                + '<div class="form-item"><label class="form-label">操作模块</label><div class="form-control-area">'
                + escapeHtml(d.typeName || d.type || '') + '</div></div>'
                + '<div class="form-item mt-4"><label class="form-label">登录信息</label><div class="form-control-area">'
                + escapeHtml((d.username || '') + (d.ip ? ' / ' + d.ip : '')) + '</div></div>'
                + '<div class="form-item mt-4"><label class="form-label">请求地址</label><div class="form-control-area">'
                + escapeHtml(d.requestUri || '无') + '</div></div>'
                + '<div class="form-item mt-4"><label class="form-label">请求参数</label><div class="form-control-area">'
                + formatJsonBlock(d.requestParam) + '</div></div>'
                + '<div class="form-item mt-4"><label class="form-label">返回参数</label><div class="form-control-area">'
                + formatJsonBlock(d.responseParam) + '</div></div>'
                + '<div class="form-item mt-4"><label class="form-label">状态 / 时间</label><div class="form-control-area">'
                + statusBadge(d.status) + ' &nbsp; ' + formatDate(d.createTime || '')
                + (d.costMs != null ? '（耗时 ' + d.costMs + 'ms）' : '')
                + '</div></div>';

            showConfirm({
                title: '操作日志详细',
                icon: 'info',
                size: 'xlarge',
                showCancel: false,
                confirmText: '关闭',
                body: body
            });
        });
    }

    const pagination = new PaginationManager({
        requestUrl: '/log/oper/query',
        tableBodySelector: '#operLogList',
        pageSize: 10,
        showBoundaryButtons: true,
        renderRow: function (row) {
            return '<tr>'
                + '<td>' + escapeHtml(row.typeName || row.type || '') + '</td>'
                + '<td class="white-space-none">' + escapeHtml(row.message || row.json || '') + '</td>'
                + '<td>' + escapeHtml(row.username || '') + '</td>'
                + '<td>' + escapeHtml(row.ip || '') + '</td>'
                + '<td>' + statusBadge(row.status) + '</td>'
                + '<td>' + formatDate(row.createTime || '') + '</td>'
                + '<td><button type="button" class="table-action-btn view log-detail-btn" data-id="'
                + escapeHtml(row.id || '') + '" title="详细"><i class="fa fa-eye"></i></button></td>'
                + '</tr>';
        },
        refreshCompleted: function () {
            $('#operLogList .log-detail-btn').off('click').on('click', function () {
                showDetail($(this).data('id'));
            });
        }
    });

    function search() {
        pagination.doSearch(collectParams());
    }

    const statusSelect = $('#filterStatus').dbSelect({
        type: 'single'
    });

    $('#searchBtn').on('click', search);
    $('#resetBtn').on('click', function () {
        $('#filterUsername').val('');
        $('#filterIp').val('');
        if (statusSelect && typeof statusSelect.setValues === 'function') {
            statusSelect.setValues(['-1'], true);
        } else {
            $('#filterStatus').val('-1');
        }
        $('#filterBeginTime').val('');
        $('#filterEndTime').val('');
        search();
    });
    $('#clearBtn').on('click', function () {
        showConfirm({
            title: '确认清空全部日志？',
            icon: 'warning',
            size: 'large',
            confirmType: 'danger',
            onConfirm: function () {
                doPoster('/log/clear', {}, function (res) {
                    if (res && res.success) {
                        bootGrowl('清空日志成功', 'success');
                        search();
                    } else {
                        bootGrowl((res && res.message) || '清空失败', 'danger');
                    }
                });
            }
        });
    });

    search();
})();
