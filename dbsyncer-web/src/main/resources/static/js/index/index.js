// 添加项目组
function bindAddGroup() {
    $("#addGroupBtn").click(function () {
        doLoader("/projectGroup/page/add");
    });
}
// 编辑项目组
function bindEditGroup() {
    $("#editGroupBtn").click(function () {
        var $id = $('#group').val();
        if (isBlank($id)) {
            bootGrowl("请选择有效项目组", "danger");
            return false;
        }
        doGetter('/projectGroup/get', {id: $id}, function (data) {
            if (data.success) {
                doLoader('/projectGroup/page/edit?id=' + $id);
            } else {
                bootGrowl(data.resultValue, 'danger');
            }
        });
    });
}
// 删除项目组
function bindRemoveGroup() {
    $("#delGroupBtn").click(function () {
        var $id = $('#group').val();
        if (isBlank($id)) {
            bootGrowl("请选择有效项目组", "danger");
            return false;
        }
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "确认删除？",
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
                        $.cookie("groupID", '');
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
// 项目组筛选
function bindGroupChange() {
    var $group = $('#group');
    // 绑定选择事件
    $group.on('change, changed.bs.select', function () {
        var groupID = $(this).val();
        $.cookie("groupID", groupID);
        searchByGroup(groupID);
    });
}

// 根据项目组查询
function searchByGroup(groupID) {
    $.loadingT(true);
    doLoader("/index?projectGroupId=" + groupID);
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

function bindConnectorDropdownMenu() {
    $(".connectorList .dropdown-menu li").click(function () {
        var $url = $(this).attr("url");
        // 如果当前为恢复状态
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "确认删除？",
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

$(function () {
    $(".select-control").selectpicker({
        "style":'dbsyncer_btn-info',
        "title":"全部",
        "actionsBox":true,
        "liveSearch":true,
        "noneResultsText":"没有找到 {0}",
        "selectedTextFormat":"count > 10"
    });
    // 初始化group
    $('#group').selectpicker('val', $('#selectedGroup').val());

    bindAddGroup();
    bindEditGroup();
    bindRemoveGroup();
    bindGroupChange();

    bindAddConnector();
    bindEditConnector();

    bindAddMapping();
    bindEditMapping();
    bindQueryData();

    bindConnectorDropdownMenu();
    bindMappingDropdownMenu();

});
