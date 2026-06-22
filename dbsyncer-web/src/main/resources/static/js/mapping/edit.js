// 修改驱动名称
function mappingModifyName() {
    const $name = $("#mapping_name_modify");
    const tmp = $name.text();
    $name.text("");
    $name.append("<input type='text' class='form-control text-md'/>");
    const $input = $name.find("input");
    $input.focus().val(tmp);
    $input.blur(function () {
        $name.text($(this).val());
        $("#mappingModifyForm input[name='name']").val($(this).val());
        $input.unbind();
    });
}

// 绑定全量+增量切换事件
function bindMappingModelChange() {
    const mappingModel = $('#mapping_model').radioGroup({
        theme: 'info',
        size: 'md',
        onChange: function(value, label) {
            showSuperConfig(value);
        }
    });
    // 渲染选择radio配置
    showSuperConfig(mappingModel.getValue());
}
function showSuperConfig(model){
    const full = $("#mapping_full");
    const increment = $("#mapping_increment");
    if ('full' === model) {
        increment.addClass("hidden");
    } else {
        increment.removeClass("hidden");
    }
}

// 绑定日志+定时切换事件
function bindIncrementConfigChange(){
    const incrementStrategy = $('#increment_strategy').radioGroup({
        theme: 'info',
        size: 'md',
        onChange: function(value, label) {
            showIncrementConfig(value);
        }
    });
    // 渲染选择radio配置
    showIncrementConfig(incrementStrategy.getValue());
}
function showIncrementConfig(model){
    const incrementQuartz = $("#increment_quartz");
    if ('log' === model) {
        incrementQuartz.addClass("hidden");
    } else {
        incrementQuartz.removeClass("hidden");
    }
}

// 修改增量点配置
function bindSnapshotModifyClick() {
    $(".metaSnapshotModify").click(function(){
        const $cell = $(this).closest('tr').find("td:eq(2)");
        const oldValue = $cell.text();
        const $input = $('<input type="text" class="form-control"/>').val(oldValue);

        $cell.html($input);
        $input.focus().select();

        $input.one('blur', function () {
            const newValue = $(this).val();
            $cell.text(newValue);
            if (oldValue !== newValue) {
                createMetaSnapshotParams();
            }
        });
    })
}
// 生成增量点配置参数
function createMetaSnapshotParams() {
    const snapshot = {};
    $("#mappingMetaSnapshotConfig tr").each(function () {
        snapshot[$(this).find("td:eq(1)").text()] = $(this).find("td:eq(2)").text();
    });
    $("#metaSnapshot").val(JSON.stringify(snapshot));
}


function onDBChange(connectorId, schemaSelect, dbName, defaultSchema) {
    schemaSelect.setData([]);
    schemaSelect.setValues([], true);
    if (!connectorId || !dbName) {
        return;
    }
    doGetter('/connector/getSchema', {id: connectorId, database: dbName}, function (response) {
        if (response.success) {
            const schemas = response.data || [];
            const array = schemas.map(function (schema) {
                return {label: schema, value: schema, disabled: false};
            });
            schemaSelect.setData(array);
            if (defaultSchema && array.some(function (item) { return item.value === defaultSchema; })) {
                schemaSelect.setValues([defaultSchema], true);
            }
        } else {
            bootGrowl("获取Schema信息失败: " + response.message, "danger");
        }
    });
}

function onConnectorChange(connectorId, dbSelect, schemaSelect, defaultDatabase, defaultSchema) {
    dbSelect.setData([]);
    dbSelect.setValues([], true);
    schemaSelect.setData([]);
    schemaSelect.setValues([], true);
    if (!connectorId) {
        return;
    }
    doGetter('/connector/getDatabase', {id: connectorId}, function (response) {
        if (response.success) {
            const databases = response.data || [];
            const db = databases.map(function (dbName) {
                return {label: dbName, value: dbName, disabled: false};
            });
            dbSelect.setData(db);
            if (defaultDatabase && db.some(function (item) { return item.value === defaultDatabase; })) {
                dbSelect.setValues([defaultDatabase], true);
                onDBChange(connectorId, schemaSelect, defaultDatabase, defaultSchema);
            }
        } else {
            bootGrowl("获取数据库信息失败: " + response.message, "danger");
        }
    });
}

function initDBSelect($connector, $database, $schema) {
    // 为每个 select 组维护独立的连接器ID，避免上下文串用
    let currentConnectorId = null;
    const defaultDatabase = $database.data("database") || '';
    const defaultSchema = $schema.data("schema") || '';

    const schemaSelect = $schema.dbSelect({
        type: 'single',
        defaultValue: defaultSchema ? [defaultSchema] : null
    });
    const dbSelect = $database.dbSelect({
        type: 'single',
        defaultValue: defaultDatabase ? [defaultDatabase] : null,
        onSelect: function (selected) {
            if (currentConnectorId) {
                onDBChange(currentConnectorId, schemaSelect, selected.length >= 1 ? selected[0] : '');
            }
        }
    });
    const connectorSelect = $connector.dbSelect({
        type: 'single',
        onSelect: function (connectorId) {
            currentConnectorId = connectorId.length >= 1 ? connectorId[0] : '';
            onConnectorChange(currentConnectorId, dbSelect, schemaSelect);
        }
    });

    // 初始化：如果有默认选中的连接器，加载库列表
    const selected = connectorSelect.getValues();
    if (selected.length >= 1) {
        currentConnectorId = selected[0];
        onConnectorChange(currentConnectorId, dbSelect, schemaSelect, defaultDatabase, defaultSchema);
    }
}

$(function () {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/mapping/list');
    };

    initDBSelect($('#sourceConnectorId'), $('#sourceDatabase'), $('#sourceSchema'));
    initDBSelect($('#targetConnectorId'), $('#targetDatabase'), $('#targetSchema'));
    // 绑定全量+增量切换事件
    bindMappingModelChange();
    // 绑定日志+定时切换事件
    bindIncrementConfigChange();
    // 绑定增量点配置编辑事件
    bindSnapshotModifyClick();

    // 保存
    $("#mappingSubmitBtn").click(function () {
        let $form = $("#mappingModifyForm");
        if (validateForm($form)) {
            const $btn = $(this);
            const originalText = $btn.html();
            $btn.html('<i class="fa fa-spinner fa-spin"></i> 保存中...').prop('disabled', true);
            doPoster("/mapping/edit", $form.serializeJson(), function (response) {
                $btn.html(originalText).prop('disabled', false);
                if (response.success === true) {
                    bootGrowl("保存成功!", "success");
                    const rawStr = localStorage.getItem("dbsyncer.pagination.mapping-list");
                    const raw = JSON.parse(rawStr);
                    raw.pageNum = 1;
                    localStorage.setItem('dbsyncer.pagination.mapping-list', JSON.stringify(raw));
                    // 返回到默认主页
                    backIndexPage();
                } else {
                    bootGrowl(response.message, "danger");
                }
            });
        }
    });

})