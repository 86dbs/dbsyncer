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

// 绑定匹配相似表复选框事件
function bindAutoMatchTableCheckBoxClick(){
    $('#autoMatchTableSelect').iCheck({
        checkboxClass: 'icheckbox_square-blue',
        labelHover: false,
        cursor: true
    });
}

$(function () {
    // 兼容IE PlaceHolder
    $('input[type="text"],input[type="password"],textarea').PlaceHolder();

    // 初始化select插件
    $(".select-control").selectpicker({
        "title":"请选择",
        "actionsBox":true,
        "liveSearch":true,
        "selectAllText":"全选",
        "deselectAllText":"取消全选",
        "noneResultsText":"没有找到 {0}",
        "selectedTextFormat":"count > 10"
    });

    // 绑定匹配相似表复选框事件
    bindAutoMatchTableCheckBoxClick();

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