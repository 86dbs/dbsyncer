$(function () {
    // 绑定添加用户按钮事件
    $("#addUserBtn").on('click', function () {
        doLoader("/user/page/add");
    });

    // 绑定修改用户按钮事件
    $(".editUserBtn").on('click', function () {
        const username = $(this).data('username');
        doLoader("/user/page/edit?username=" + username);
    });

    // 绑定删除用户按钮事件
    $(".removeUserBtn").on('click', function () {
        const username = $(this).data('username');
        const $btn = $(this);
        
        // 原生确认对话框
        if (confirm('确认删除用户 "' + username + '" 吗？此操作不可恢复。')) {
            // 禁用按钮，防止重复点击
            $btn.prop('disabled', true);
            const originalText = $btn.html();
            $btn.html('<i class="fa fa-spinner fa-spin"></i> 删除中...');
            
            doPoster('/user/remove', { username: username }, function (data) {
                $btn.prop('disabled', false);
                $btn.html(originalText);
                if (data.success === true) {
                    bootGrowl("删除成功！", "success");
                    doLoader("/user?refresh=" + new Date().getTime());
                } else {
                    bootGrowl(data.resultValue || "删除失败", "danger");
                }
            });
        }
    });
})