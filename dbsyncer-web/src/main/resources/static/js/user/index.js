$(function () {
    // 绑定添加用户按钮事件
    $("#addUserBtn").click(function () {
        doLoader("/user/page/add");
    });

    // 绑定修改用户按钮事件
    $(".editUserBtn").click(function () {
        doLoader("/user/page/edit?username=" + $(this).attr("id"));
    });

    // 绑定删除用户按钮事件
    $(".removeUserBtn").click(function () {
        const $username = $(this).attr("id");
        // 确认框确认是否删除用户
        BootstrapDialog.show({
            title: "提示",
            type: BootstrapDialog.TYPE_INFO,
            message: "确认删除帐号？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster('/user/remove', {username: $username}, function (data) {
                        if (data.success == true) {
                            bootGrowl("删除用户成功！", "success");
                        } else {
                            bootGrowl(data.resultValue, "danger");
                        }
                        doLoader("/user?refresh=" + new Date().getTime());
                    });
                    dialog.close();
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });

})