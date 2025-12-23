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
        full.removeClass("hidden");
    } else {
        full.addClass("hidden");
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

$(function () {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/mapping/list');
    };

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
            doPoster("/mapping/edit", $form.serializeJson(), function (response) {
                if (response.success === true) {
                    bootGrowl("修改驱动成功!", "success");
                    // 返回到默认主页
                    backIndexPage();
                } else {
                    bootGrowl(response.message, "danger");
                }
            });
        }
    });

})