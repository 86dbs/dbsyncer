/**
 * Agent 新增/编辑表单
 */
function initAgentForm(options) {
    'use strict';
    options = options || {};
    var mode = options.mode || 'add';
    var agent = options.agent || null;

    window.backIndexPage = function () {
        doLoader('/agent/list');
    };

    if (agent && agent.json) {
        try {
            var cfg = typeof agent.json === 'string' ? JSON.parse(agent.json) : agent.json;
            if (cfg) {
                $('#jdbcUrl').val(cfg.jdbcUrl || '');
                $('#username').val(cfg.username || '');
                $('#password').val(cfg.password || '');
                $('#schema').val(cfg.schema || '');
            }
        } catch (e) {
            // ignore
        }
    }

    $('#genSecretBtn').on('click', function () {
        doPoster('/agent/generateSecret', {}, function (res) {
            if (res.success) {
                $('#agentSecret').val(res.data);
            } else {
                alert(res.message || '生成失败');
            }
        });
    });

    $('#saveBtn').on('click', function () {
        var name = $.trim($('#name').val() || '');
        var listenPort = $.trim($('#listenPort').val() || '');
        var agentSecret = $.trim($('#agentSecret').val() || '');
        var jdbcUrl = $.trim($('#jdbcUrl').val() || '');
        var username = $.trim($('#username').val() || '');
        var password = $('#password').val() || '';
        var schema = $.trim($('#schema').val() || '');
        if (!name || !listenPort || !agentSecret || !jdbcUrl || !username) {
            alert('请填写必填项');
            return;
        }
        if (mode === 'add' && !password) {
            alert('请填写密码');
            return;
        }
        var params = {
            name: name,
            listenPort: listenPort,
            agentSecret: agentSecret,
            threadNum: $('#threadNum').val() || '4',
            captureIntervalMs: $('#captureIntervalMs').val() || '1000',
            checkpoint: $('#checkpoint').val() || 'tail.ckpt',
            jdbcUrl: jdbcUrl,
            username: username,
            password: password,
            schema: schema
        };
        if (mode === 'edit') {
            params.id = $('#id').val();
        }
        var url = mode === 'edit' ? '/agent/edit' : '/agent/add';
        doPoster(url, params, function (res) {
            if (res.success) {
                backIndexPage();
            } else {
                alert(res.message || '保存失败');
            }
        });
    });
}
