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

    const pagination = new PaginationManager({
        requestUrl: '/log/login/query',
        tableBodySelector: '#loginLogList',
        pageSize: 10,
        customPageSize: true,
        customPageSizeItems: [5, 10, 20, 50, 100],
        storageKey: 'log.login',
        showBoundaryButtons: true,
        renderRow: function (row) {
            return '<tr>'
                + '<td>' + (row.username || '') + '</td>'
                + '<td>' + (row.ip || '') + '</td>'
                + '<td>' + statusBadge(row.status) + '</td>'
                + '<td class="white-space-none">' + (row.message || row.json || '') + '</td>'
                + '<td>' + formatDate(row.createTime || '') + '</td>'
                + '</tr>';
        }
    });

    function search() {
        pagination.doSearch(collectParams(), 1);
    }

    const statusSelect = $('#filterStatus').dbSelect({
        type: 'single',
        onSelect: function () {
            search();
        }
    });

    $('#filterUsername, #filterIp').on('blur', search);
    $('#filterBeginTime, #filterEndTime').on('change', search);
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
})();
