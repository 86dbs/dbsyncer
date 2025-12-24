/**
 * 任务列表页面
 */

// 全局变量
let taskPagination = null;

/**
 * 渲染任务状态
 */
function renderTaskStatus(status) {
    const statusMap = {
        '0': '<span class="badge badge-info">未启动</span>',
        '1': '<span class="badge badge-success">运行中</span>',
        '2': '<span class="badge badge-warning">已停止</span>',
        '3': '<span class="badge badge-error">异常</span>'
    };
    return statusMap[status] || '<span class="badge">未知</span>';
}

/**
 * 渲染任务类型
 */
function renderTaskType(type) {
    const typeMap = {
        'SYNC': '<span class="badge badge-primary">同步</span>',
        'FULL': '<span class="badge badge-secondary">全量</span>',
        'INCREMENT': '<span class="badge badge-info">增量</span>'
    };
    return typeMap[type] || '<span class="badge">' + (type || '未知') + '</span>';
}

/**
 * 渲染操作按钮
 */
function renderTaskActions(task) {
    let actions = [];
    
    // 编辑按钮
    actions.push(`
        <button class="btn btn-sm btn-outline edit-task-btn" data-task-id="${task.id || ''}" title="编辑">
            <i class="fa fa-edit"></i>
        </button>
    `);
    
    // 启动/停止按钮
    const status = task.status || '0';
    if (status === '0' || status === '2') {
        actions.push(`
            <button class="btn btn-sm btn-success start-task-btn" data-task-id="${task.id || ''}" title="启动">
                <i class="fa fa-play"></i>
            </button>
        `);
    } else if (status === '1') {
        actions.push(`
            <button class="btn btn-sm btn-warning stop-task-btn" data-task-id="${task.id || ''}" title="停止">
                <i class="fa fa-stop"></i>
            </button>
        `);
    }
    
    // 删除按钮
    actions.push(`
        <button class="btn btn-sm btn-danger delete-task-btn" data-task-id="${task.id || ''}" title="删除">
            <i class="fa fa-trash"></i>
        </button>
    `);
    
    return actions.join(' ');
}

/**
 * 格式化日期时间
 */
function formatDateTime(dateTimeStr) {
    if (!dateTimeStr) return '-';
    try {
        const date = new Date(dateTimeStr);
        if (isNaN(date.getTime())) return dateTimeStr;
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        const seconds = String(date.getSeconds()).padStart(2, '0');
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (e) {
        return dateTimeStr;
    }
}

/**
 * 初始化任务列表
 */
function initTaskList() {
    console.log('[任务列表] 开始初始化');
    
    // 获取搜索关键词
    function getSearchParams() {
        const keyword = $('#searchKeyword').val() || '';
        return {
            keyword: keyword.trim()
        };
    }
    
    // 初始化分页管理器
    taskPagination = new PaginationManager({
        requestUrl: '/task/list',
        tableBodySelector: '#taskTableBody',
        paginationSelector: '#taskPagination',
        countSelector: '#totalCount',
        currentPageSelector: '#currentPage',
        totalPagesSelector: '#totalPages',
        pageSize: 10,
        params: getSearchParams(),
        renderRow: function(task, index) {
            const taskName = escapeHtml(task.name || '未命名任务');
            const taskType = task.type || 'SYNC';
            const taskStatus = task.status || '0';
            const updateTime = formatDateTime(task.updateTime);
            
            return `
                <tr>
                    <td>
                        <input type="checkbox" class="task-checkbox" data-task-id="${task.id || ''}" />
                    </td>
                    <td>
                        <div class="font-medium">${taskName}</div>
                        ${task.id ? '<div class="text-xs text-gray-500">ID: ' + escapeHtml(task.id) + '</div>' : ''}
                    </td>
                    <td>${renderTaskType(taskType)}</td>
                    <td>${renderTaskStatus(taskStatus)}</td>
                    <td>
                        <span class="text-sm text-gray-600">-</span>
                    </td>
                    <td>${updateTime}</td>
                    <td>
                        <div class="flex items-center gap-1">
                            ${renderTaskActions(task)}
                        </div>
                    </td>
                </tr>
            `;
        },
        emptyHtml: `
            <tr>
                <td colspan="7" class="text-center">
                    <i class="fa fa-history empty-icon"></i>
                    <p class="empty-text">暂无任务</p>
                    <p class="empty-description">点击"添加"按钮创建第一个任务</p>
                </td>
            </tr>
        `,
        refreshCompleted: function() {
            bindTaskEvents();
        }
    });
    
    console.log('[任务列表] 分页管理器初始化完成');
}

/**
 * 绑定任务操作事件
 */
function bindTaskEvents() {
    // 编辑任务
    $('.edit-task-btn').off('click').on('click', function() {
        const taskId = $(this).data('task-id');
        console.log('[任务列表] 编辑任务:', taskId);
        if (taskId) {
            doLoader('/task/page/edit?id=' + taskId);
        } else {
            bootGrowl('任务ID不存在', 'warning');
        }
    });
    
    // 启动任务
    $('.start-task-btn').off('click').on('click', function() {
        const taskId = $(this).data('task-id');
        console.log('[任务列表] 启动任务:', taskId);
        if (taskId) {
            showConfirm({
                title: '确认启动任务？',
                icon: 'info',
                size: 'medium',
                confirmType: 'primary',
                onConfirm: function() {
                    doPoster('/task/start', {taskId: taskId}, function(response) {
                        if (response.success) {
                            bootGrowl('任务启动成功！', 'success');
                            if (taskPagination) {
                                const params = taskPagination.searchParams || {};
                                taskPagination.doSearch(params, taskPagination.currentPage);
                            }
                        } else {
                            bootGrowl('任务启动失败: ' + response.resultValue, 'danger');
                        }
                    });
                }
            });
        }
    });
    
    // 停止任务
    $('.stop-task-btn').off('click').on('click', function() {
        const taskId = $(this).data('task-id');
        console.log('[任务列表] 停止任务:', taskId);
        if (taskId) {
            showConfirm({
                title: '确认停止任务？',
                icon: 'warning',
                size: 'medium',
                confirmType: 'warning',
                onConfirm: function() {
                    doPoster('/task/stop', {taskId: taskId}, function(response) {
                        if (response.success) {
                            bootGrowl('任务已停止！', 'success');
                            if (taskPagination) {
                                const params = taskPagination.searchParams || {};
                                taskPagination.doSearch(params, taskPagination.currentPage);
                            }
                        } else {
                            bootGrowl('任务停止失败: ' + response.resultValue, 'danger');
                        }
                    });
                }
            });
        }
    });
    
    // 删除任务
    $('.delete-task-btn').off('click').on('click', function() {
        const taskId = $(this).data('task-id');
        console.log('[任务列表] 删除任务:', taskId);
        if (taskId) {
            showConfirm({
                title: '确认删除任务？',
                icon: 'warning',
                size: 'medium',
                confirmType: 'danger',
                onConfirm: function() {
                    doGetter('/task/delete', {taskId: taskId}, function(response) {
                        if (response.success) {
                            bootGrowl('任务删除成功！', 'success');
                            if (taskPagination) {
                                const params = taskPagination.searchParams || {};
                                taskPagination.doSearch(params, taskPagination.currentPage);
                            }
                        } else {
                            bootGrowl('任务删除失败: ' + response.resultValue, 'danger');
                        }
                    });
                }
            });
        }
    });
}

/**
 * 搜索任务
 */
function searchTasks() {
    if (taskPagination) {
        const params = {
            keyword: $('#searchKeyword').val() || ''
        };
        // 保存搜索参数供后续使用
        taskPagination.searchParams = params;
        taskPagination.doSearch(params, 1);
    }
}

// 页面加载完成后初始化
$(document).ready(function() {
    console.log('[任务列表] 页面加载完成');
    
    // 初始化任务列表
    initTaskList();
    
    // 搜索框回车事件
    $('#searchKeyword').on('keypress', function(e) {
        if (e.which === 13) {
            e.preventDefault();
            searchTasks();
        }
    });
    
    // 搜索框清除按钮
    $('.search-clear').on('click', function() {
        $('#searchKeyword').val('');
        searchTasks();
    });
    
    // 添加任务按钮（已在 list.html 中绑定）
    // $('#addTaskBtn').click(function() {
    //     doLoader('/task/page/add');
    // });
});

