function submit(data) {
    doPoster("/mapping/add", data, function (data) {
        if (data.success == true) {
            bootGrowl("新增驱动成功!", "success");
            // 保存成功后返回到同步任务管理页面，而不是编辑页面
            if (typeof loadMappingListPage === 'function') {
                // 返回到同步任务管理页面
                loadMappingListPage();
            } else if (typeof doLoader === 'function') {
                // 兜底方案：使用doLoader加载同步任务列表
                doLoader('/mapping/list', 0);
            } else {
                // 如果都不存在，直接跳转
                window.location.href = '/mapping/list';
            }
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

    // 分别初始化两个select插件，避免状态冲突
    var $sourceSelect = $("select[name='sourceConnectorId']");
    var $targetSelect = $("select[name='targetConnectorId']");
    
    initSelectIndex($sourceSelect, 0);
    initSelectIndex($targetSelect, 0);
   

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
        // 优先返回到同步任务管理页面，如果该函数不存在则返回到默认主页
        if (typeof loadMappingListPage === 'function') {
            // 返回到同步任务管理页面
            loadMappingListPage();
        } else if (typeof backIndexPage === 'function') {
            // 返回到默认主页
            backIndexPage();
        } else {
            // 最后的兜底方案：使用doLoader加载同步任务列表
            if (typeof doLoader === 'function') {
                doLoader('/mapping/list', 0);
            } else {
                // 如果都不存在，直接跳转
                window.location.href = '/mapping/list';
            }
        }
    });
})