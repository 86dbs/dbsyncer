// 添加分组
function bindAddProjectGroup() {
    $("#addProjectGroupBtn").click(function () {
        doLoader("/projectGroup/page/add");
    });
}

// 修改分组
function bindEditProjectGroup($projectGroupSelect) {
    $("#editProjectGroupBtn").click(function () {
        var $id = $projectGroupSelect.selectpicker('val');
        if (!isBlank($id)) {
            doLoader('/projectGroup/page/edit?id=' + $id);
            return;
        }
        bootGrowl("请选择分组", "danger");
    });
}

// 删除分组
function bindRemoveProjectGroup($projectGroupSelect) {
    $("#removeProjectGroupBtn").click(function () {
        var $id = $projectGroupSelect.selectpicker('val');
        if (isBlank($id)) {
            bootGrowl("请选择分组", "danger");
            return;
        }
        BootstrapDialog.show({
            title: "提示",
            type: BootstrapDialog.TYPE_INFO,
            message: "确认删除分组？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster('/projectGroup/remove',{id: $id}, function (data) {
                        if (data.success == true) {
                            // 显示主页
                            backIndexPage();
                            bootGrowl(data.resultValue, "success");
                        } else {
                            bootGrowl(data.resultValue, "danger");
                        }
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
}

// 给分组下拉绑定事件
function bindProjectGroupSelect($projectGroupSelect) {
    // 绑定选择事件
    $projectGroupSelect.on('change, changed.bs.select', function () {
        $.loadingT(true);
        doLoader("/index?projectGroupId=" + $(this).val());
    });
}

// 添加连接
function bindAddConnector() {
    // 绑定添加连接按钮点击事件
    $("#indexAddConnectorBtn").click(function () {
        doLoader('/connector/page/add');
    });
}

// 编辑连接
function bindEditConnector() {
    $(".connectorList .dbsyncer_block").click(function () {
        var $id = $(this).attr("id");
        doLoader('/connector/page/edit?id=' + $id);
    });
}

// 添加驱动
function bindAddMapping() {
    $("#indexAddMappingBtn").click(function () {
        doLoader('/mapping/pageAdd');
    });
}

// 编辑驱动
function bindEditMapping() {
    $(".mappingList .dbsyncer_block").click(function () {
        var $id = $(this).attr("id");
        doLoader('/mapping/page/edit?id=' + $id);
    });
}

// 查看驱动日志
function bindQueryData() {
    $(".mappingList .queryData").click(function () {
        // 阻止触发click传递事件
        event.cancelBubble=true;
        var $menu = $('#menu > li');
        $menu.removeClass('active');
        $menu.find("a[url='/monitor']").parent().addClass('active');

        var $id = $(this).attr("id");
        doLoader('/monitor?id=' + $id);
    });
}

// 给连接下拉菜单绑定事件
function bindConnectorDropdownMenu() {
    // 绑定删除连接事件
    $(".connectorList .dropdown-menu li.remove").click(function () {
        var $url = $(this).attr("url");
        // 如果当前为恢复状态
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "确认删除连接？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPost($url);
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
    // 绑定复制连接事件
    $(".connectorList .dropdown-menu li.copy").click(function () {
        var $url = $(this).attr("url");
        doPost($url);
    });
}

// 给驱动下拉菜单绑定事件
function bindMappingDropdownMenu() {
    $(".mappingList .dropdown-menu li").click(function () {
        var $url = $(this).attr("url");
        var $confirm = $(this).attr("confirm");
        var $confirmMessage = $(this).attr("confirmMessage");

        if ("true" == $confirm) {
            // 如果当前为恢复状态
            BootstrapDialog.show({
                title: "警告",
                type: BootstrapDialog.TYPE_DANGER,
                message: $confirmMessage,
                size: BootstrapDialog.SIZE_NORMAL,
                buttons: [{
                    label: "确定",
                    action: function (dialog) {
                        doPost($url);
                        dialog.close();
                    }
                }, {
                    label: "取消",
                    action: function (dialog) {
                        dialog.close();
                    }
                }]
            });
            return;
        }

        doPost($url);
    });
}

function doPost(url) {
    doPoster(url, null, function(data){
        if (data.success == true) {
            // 显示主页
            backIndexPage();
            bootGrowl(data.resultValue, "success");
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

// 创建定时器
function createTimer($projectGroupSelect){
    doGetWithoutLoading("/monitor/getRefreshIntervalSeconds",{}, function (data) {
        if (data.success == true) {
            timer = setInterval(function(){
                backIndexPage($projectGroupSelect.selectpicker('val'));
            }, data.resultValue * 1000);
        } else {
            bootGrowl(data.resultValue, "danger");
        }
    });
}

$(function () {
    // 初始化select插件
    initSelectIndex($(".select-control"));
    bindAddProjectGroup();
    var $projectGroupSelect = $("#projectGroup");
    bindEditProjectGroup($projectGroupSelect);
    bindRemoveProjectGroup($projectGroupSelect);
    bindProjectGroupSelect($projectGroupSelect);
    createTimer($projectGroupSelect);

    bindAddConnector();
    bindEditConnector();

    bindAddMapping();
    bindEditMapping();
    bindQueryData();

    bindConnectorDropdownMenu();
    bindMappingDropdownMenu();
});