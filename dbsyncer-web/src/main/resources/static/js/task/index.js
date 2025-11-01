$(function () {
    // 默认加载第一页
    loadTaskList(1, 10);

    // 绑定创建任务按钮事件
    $("#addTaskBtn").click(function () {
        doLoader("/task/page/add");
    });
});

/**
 * 加载任务列表
 * @param pageNum 页码
 * @param pageSize 每页条数
 */
function loadTaskList(pageNum, pageSize) {
    const params = {
        pageNum: pageNum + "",
        pageSize: pageSize + ""
    };
    doPoster("/task/list", params, function (data) {
        if (data.success == true) {
            renderTaskList(data.resultValue);
            renderPagination(data.total, pageNum, pageSize);
        }
    });
}

/**
 * 渲染任务列表
 * @param tasks 任务数据
 */
function renderTaskList(tasks) {
    const $tbody = $("#taskList");
    $tbody.empty();

    if (!tasks || tasks.length === 0) {
        $tbody.append('<tr><td colspan="7" class="text-center">暂无数据</td></tr>');
        return;
    }

    tasks.forEach(function (task) {
        const statusText = getStatusText(task.status);
        const statusClass = getStatusClass(task.status);
        
        const row = '<tr>' +
            '<td>' + (task.id || '') + '</td>' +
            '<td>' + (task.name || '') + '</td>' +
            '<td>' + (task.type || '') + '</td>' +
            '<td><span class="label ' + statusClass + '">' + statusText + '</span></td>' +
            '<td>' + formatTime(task.createTime) + '</td>' +
            '<td>' + formatTime(task.updateTime) + '</td>' +
            '<td>' +
            '<button type="button" class="btn btn-primary btn-sm editTaskBtn" data-id="' + task.id + '">' +
            '<span class="fa fa-pencil"></span>修改' +
            '</button> ' +
            '<button type="button" class="btn btn-danger btn-sm deleteTaskBtn" data-id="' + task.id + '">' +
            '<span class="fa fa-times"></span>删除' +
            '</button>' +
            '</td>' +
            '</tr>';
        $tbody.append(row);
    });

    // 绑定修改按钮事件
    $(".editTaskBtn").click(function () {
        const taskId = $(this).data("id");
        // TODO: 跳转到编辑页面
        bootGrowl("编辑功能开发中...", "info");
    });

    // 绑定删除按钮事件
    $(".deleteTaskBtn").click(function () {
        const taskId = $(this).data("id");
        BootstrapDialog.show({
            title: "提示",
            type: BootstrapDialog.TYPE_WARNING,
            message: "确认删除该任务？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doGetter('/task/delete', {taskId: taskId}, function (data) {
                        if (data.success == true) {
                            bootGrowl("删除成功！", "success");
                            loadTaskList(1, 10);
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

/**
 * 渲染分页
 * @param total 总条数
 * @param pageNum 当前页码
 * @param pageSize 每页条数
 */
function renderPagination(total, pageNum, pageSize) {
    const $pagination = $("#taskPagination");
    $pagination.empty();

    if (total === 0) {
        return;
    }

    const totalPages = Math.ceil(total / pageSize);

    // 上一页
    if (pageNum > 1) {
        $pagination.append('<li><a href="javascript:void(0);" onclick="loadTaskList(' + (pageNum - 1) + ', ' + pageSize + ')">上一页</a></li>');
    } else {
        $pagination.append('<li class="disabled"><span>上一页</span></li>');
    }

    // 页码
    let startPage = Math.max(1, pageNum - 2);
    let endPage = Math.min(totalPages, pageNum + 2);

    if (startPage > 1) {
        $pagination.append('<li><a href="javascript:void(0);" onclick="loadTaskList(1, ' + pageSize + ')">1</a></li>');
        if (startPage > 2) {
            $pagination.append('<li class="disabled"><span>...</span></li>');
        }
    }

    for (let i = startPage; i <= endPage; i++) {
        if (i === pageNum) {
            $pagination.append('<li class="active"><span>' + i + '</span></li>');
        } else {
            $pagination.append('<li><a href="javascript:void(0);" onclick="loadTaskList(' + i + ', ' + pageSize + ')">' + i + '</a></li>');
        }
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            $pagination.append('<li class="disabled"><span>...</span></li>');
        }
        $pagination.append('<li><a href="javascript:void(0);" onclick="loadTaskList(' + totalPages + ', ' + pageSize + ')">' + totalPages + '</a></li>');
    }

    // 下一页
    if (pageNum < totalPages) {
        $pagination.append('<li><a href="javascript:void(0);" onclick="loadTaskList(' + (pageNum + 1) + ', ' + pageSize + ')">下一页</a></li>');
    } else {
        $pagination.append('<li class="disabled"><span>下一页</span></li>');
    }

    // 显示总数
    $pagination.append('<li class="disabled"><span>共 ' + total + ' 条</span></li>');
}

/**
 * 获取状态文本
 * @param status 状态码
 * @returns {string} 状态文本
 */
function getStatusText(status) {
    switch (status) {
        case 0:
            return "未执行";
        case 1:
            return "执行中";
        case 2:
            return "执行成功";
        case 3:
            return "执行失败";
        default:
            return "未知";
    }
}

/**
 * 获取状态样式
 * @param status 状态码
 * @returns {string} 样式类名
 */
function getStatusClass(status) {
    switch (status) {
        case 0:
            return "label-default";
        case 1:
            return "label-primary";
        case 2:
            return "label-success";
        case 3:
            return "label-danger";
        default:
            return "label-default";
    }
}

/**
 * 格式化时间
 * @param timestamp 时间戳对象
 * @returns {string} 格式化后的时间
 */
function formatTime(timestamp) {
    if (!timestamp) {
        return '';
    }
    
    // 如果是 Timestamp 对象，提取时间
    if (timestamp.time) {
        const date = new Date(timestamp.time);
        return formatDate(date);
    }
    
    // 如果是数字，直接转换
    if (typeof timestamp === 'number') {
        const date = new Date(timestamp);
        return formatDate(date);
    }
    
    // 如果是字符串，尝试解析
    if (typeof timestamp === 'string') {
        return timestamp;
    }
    
    return '';
}

/**
 * 格式化日期对象
 * @param date 日期对象
 * @returns {string} 格式化后的日期字符串
 */
function formatDate(date) {
    const year = date.getFullYear();
    const month = padZero(date.getMonth() + 1);
    const day = padZero(date.getDate());
    const hours = padZero(date.getHours());
    const minutes = padZero(date.getMinutes());
    const seconds = padZero(date.getSeconds());
    return year + '-' + month + '-' + day + ' ' + hours + ':' + minutes + ':' + seconds;
}

/**
 * 补零
 * @param num 数字
 * @returns {string} 补零后的字符串
 */
function padZero(num) {
    return num < 10 ? '0' + num : num;
}

