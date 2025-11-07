function submitConnector(data) {
    var $btn = $("#connectorSubmitBtn");
    if (!$btn.length) {
        return;
    }

    if ($btn.prop('disabled')) {
        return;
    }

    var originalText = $btn.html();
    $btn.html('<i class="fa fa-spinner fa-spin"></i> 保存中...').prop('disabled', true);

    doPoster("/connector/add", data, function (response) {
        $btn.html(originalText).prop('disabled', false);
        if (response.success === true) {
            bootGrowl("新增连接成功!", "success");
            backIndexPage();
        } else {
            bootGrowl(response.resultValue || '添加连接失败', "danger");
        }
    });
}

window.submitConnector = submitConnector;

// 绑定连接器类型切换事件
function bindConnectorChangeEvent($select) {
    if (!$select || !$select.length) {
        return;
    }

    changeConnectorType($select);

    $select.off('change.connector').on('change.connector', function () {
        changeConnectorType($select);
    });
}

function changeConnectorType($select) {
    if (!$select || !$select.length) {
        return;
    }

    var connType = $select.val();
    var $connectorConfig = $("#connectorConfig");
    if (!$connectorConfig.length) {
        return;
    }

    if (!connType) {
        $connectorConfig.html('<div class="empty"><div class="empty-icon"><i class="fa fa-cog"></i></div><div class="empty-text">请选择连接类型</div></div>');
        return;
    }

    $connectorConfig.html('<div class="loading-container"><div class="loading-spinner"></div><div class="loading-text">加载配置中...</div></div>');

    $connectorConfig.load($basePath + "/connector/page/add" + connType, function (response, status) {
        if (status !== 'success') {
            $connectorConfig.html('<div class="empty"><div class="empty-icon"><i class="fa fa-warning"></i></div><div class="empty-text">加载配置失败，请稍后重试</div></div>');
        }
    });
}

function setupConnectorAddForm() {
    var $form = $("#connectorAddForm");
    if (!$form.length) {
        return;
    }

    var $select = $("#connectorType");
    bindConnectorChangeEvent($select);
}

window.setupConnectorAddForm = setupConnectorAddForm;

$(function () {
    setupConnectorAddForm();
});