/**
 * 采集任务表单
 */
(function (window) {
    'use strict';

    function resolveCaptureType() {
        var $type = $('#type');
        if ($type.length && $type.val()) {
            return $type.val();
        }
        return 'captureLocal';
    }

    function listUrl(type) {
        return type === 'captureRemote' ? '/capture/remote/list' : '/capture/local/list';
    }

    window.backIndexPage = function () {
        doLoader(listUrl(resolveCaptureType()));
    };

    window.initCaptureTaskForm = function (options) {
        options = options || {};
        var mode = options.mode || 'add';
        var $agentSecret = $('#agentSecret');
        var $copySecretBtn = $('#copySecretBtn');
        var $genSecretBtn = $('#genSecretBtn');
        var $connectorType = $('#connectorType');

        // 与连接器/用户表单一致，使用系统 dbSelect
        if ($connectorType.length) {
            if (typeof initSelect === 'function') {
                initSelect($connectorType);
            } else {
                $connectorType.dbSelect({type: 'single'});
            }
        }

        $copySecretBtn.off('click').on('click', function () {
            var text = $agentSecret.val();
            if (typeof isBlank === 'function' ? isBlank(text) : !text) {
                bootGrowl('密钥为空，无法复制', 'warning');
                return;
            }
            copyToClipboard(text, {
                button: $(this),
                successMessage: '复制密钥成功！',
                originalText: '复制',
                originalIcon: 'fa-copy'
            });
        });

        $genSecretBtn.off('click').on('click', function () {
            var $btn = $(this);
            var originalText = $btn.html();
            var captureType = resolveCaptureType();
            $btn.html('<i class="fa fa-spinner fa-spin"></i> 生成中...').prop('disabled', true);
            doPoster('/capture/generateSecret', {type: captureType}, function (res) {
                $btn.html(originalText).prop('disabled', false);
                if (res.success && res.data) {
                    $agentSecret.val(res.data);
                    bootGrowl('生成密钥成功!', 'success');
                } else {
                    bootGrowl(res.message || '生成失败', 'danger');
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
            var url = mode === 'edit' ? '/capture/edit' : '/capture/add';
            doPoster(url, params, function (res) {
                if (res.success) {
                    backIndexPage();
                } else {
                    bootGrowl(res.message || '保存失败', 'danger');
                }
            });
        });
    };
})(window);
