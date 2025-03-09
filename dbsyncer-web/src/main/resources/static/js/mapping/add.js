function submit(data) {
    doPoster("/mapping/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增驱动成功!", "success");
            doLoader('/mapping/page/edit?id=' + data.resultValue);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 绑定开关切换事件
function bindToggleSwitch($switch, $toggle) {
    let $textarea = $toggle.find("textarea");
    return $switch.bootstrapSwitch({
        onText: "Yes",
        offText: "No",
        onColor: "success",
        offColor: "info",
        size: "normal",
        onSwitchChange: function (event, state) {
            if (state) {
                $textarea.attr('tmp', $textarea.val());
                $textarea.val('');
                $toggle.addClass("hidden");
            } else {
                $textarea.val($textarea.attr('tmp'));
                $textarea.removeAttr('tmp');
                $toggle.removeClass("hidden");
            }
        }
    });
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    initSelectIndex($(".select-control"), 1);

    // 绑定匹配相似表复选框事件
    bindToggleSwitch($('#autoMatchTable'), $("#tableGroups"));

    //保存
    $("#mappingSubmitBtn").click(function () {
        var $form = $("#mappingAddForm");
        if ($form.formValidate() == true) {
            var data = $form.serializeJson();
            submit(data);
        }
    });

    //返回
    $("#mappingBackBtn").click(function () {
        backIndexPage();
    });
})