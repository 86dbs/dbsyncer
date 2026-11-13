/**
 * 采集任务表单
 */
(function (window) {
    'use strict';

    window.backIndexPage = function () {
        doLoader('/capture-task/list');
    };

    window.initCaptureTaskForm = function (options) {
        options = options || {};
        var mode = options.mode || 'add';

        $('#genSecretBtn').off('click').on('click', function () {
            doPoster('/capture-task/generateSecret', {}, function (res) {
                if (res.success) {
                    $('#agentSecret').val(res.data);
                } else {
                    alert(res.message || '生成失败');
                }
            });
        });

        $('#saveBtn').off('click').on('click', function () {
            var form = document.getElementById('captureTaskForm');
            if (form && typeof form.reportValidity === 'function' && !form.reportValidity()) {
                return;
            }
            var params = {};
            $('#captureTaskForm').serializeArray().forEach(function (item) {
                params[item.name] = item.value;
            });
            var url = mode === 'edit' ? '/capture-task/edit' : '/capture-task/add';
            doPoster(url, params, function (res) {
                if (res.success) {
                    backIndexPage();
                } else {
                    alert(res.message || '保存失败');
                }
            });
        });
    };
})(window);
