/**
 * 任务执行结果详情页面
 */

// 全局变量
let resultPagination = null;
let resultDataMap = {}; // 存储结果数据的映射表 {id: resultData}

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
 * 解析并渲染执行结果
 */
function renderResultContent(content) {
    if (!content) {
        return '<span class="text-gray-500">无结果</span>';
    }
    
    try {
        // 尝试解析JSON
        const contentObj = typeof content === 'string' ? JSON.parse(content) : content;
        
        if (Array.isArray(contentObj)) {
            const count = contentObj.length;
            if (count === 0) {
                return '<span class="badge badge-success"><i class="fa fa-check"></i> 校验通过</span>';
            } else {
                return `
                    <div class="flex items-center gap-2">
                        <span class="badge badge-warning">
                            <i class="fa fa-exclamation-triangle"></i> 发现 ${count} 个差异
                        </span>
                        <button class="btn btn-sm btn-primary view-detail-btn" title="查看详情">
                            <i class="fa fa-eye"></i> 查看详情
                        </button>
                    </div>
                `;
            }
        } else {
            // 非数组格式，直接显示摘要
            return '<span class="text-gray-600 text-sm">' + escapeHtml(JSON.stringify(contentObj).substring(0, 100)) + '...</span>';
        }
    } catch (e) {
        // 解析失败，显示原始文本（截断）
        const contentStr = String(content);
        if (contentStr.length > 100) {
            return '<span class="text-gray-600 text-sm">' + escapeHtml(contentStr.substring(0, 100)) + '...</span>';
        }
        return '<span class="text-gray-600 text-sm">' + escapeHtml(contentStr) + '</span>';
    }
}

/**
 * 显示详情弹窗
 */
function showDetailModal(result) {
    console.log('[任务执行结果] 显示详情弹窗:', result);
    
    // 设置基本信息
    $('#detailSourceTable').text(result.sourceTableName || '-');
    $('#detailTargetTable').text(result.targetTableName || '-');
    $('#detailUpdateTime').text(formatDateTime(result.updateTime));
    
    // 解析并显示详细内容
    const contentTable = $('#detailContentTable');
    contentTable.empty();
    
    try {
        const content = typeof result.content === 'string' ? JSON.parse(result.content) : result.content;
        
        if (Array.isArray(content) && content.length > 0) {
            content.forEach(function(item) {
                const key = escapeHtml(item.key || '-');
                const source = escapeHtml(item.source || '(空)');
                const target = escapeHtml(item.target || '(空)');
                const msg = escapeHtml(item.msg || '-');
                
                const row = `
                    <tr>
                        <td><strong>${key}</strong></td>
                        <td class="text-gray-600">${source}</td>
                        <td class="text-gray-600">${target}</td>
                        <td>
                            <span class="badge badge-warning">${msg}</span>
                        </td>
                    </tr>
                `;
                contentTable.append(row);
            });
        } else {
            contentTable.append(`
                <tr>
                    <td colspan="4" class="text-center text-gray-500">
                        <i class="fa fa-check-circle" style="font-size: 48px; margin-bottom: 10px; color: #10b981;"></i>
                        <p>校验通过，无差异</p>
                    </td>
                </tr>
            `);
        }
    } catch (e) {
        console.error('[任务执行结果] 解析内容失败:', e);
        contentTable.append(`
            <tr>
                <td colspan="4" class="text-center text-gray-500">
                    <i class="fa fa-exclamation-circle" style="color: #ef4444;"></i>
                    内容解析失败
                </td>
            </tr>
        `);
    }
    
    // 显示弹窗
    $('#resultDetailModal').show();
}

/**
 * 初始化结果列表
 */
function initResultList(taskId) {
    console.log('[任务执行结果] 开始初始化，任务ID:', taskId);
    
    if (!taskId) {
        bootGrowl('任务ID不存在', 'danger');
        return;
    }
    
    // 显示任务ID
    $('#taskIdDisplay').text(taskId);
    
    // 获取并显示任务信息
    doGetter('/task/getTask', {taskId: taskId}, function(response) {
        if (response.success && response.data) {
            const task = response.data;
            $('#taskName').text(task.name || '-');
        } else {
            $('#taskName').text('任务不存在或已删除');
            bootGrowl('获取任务信息失败: ' + (response.resultValue || '未知错误'), 'warning');
        }
    });
    
    // 初始化分页管理器
    resultPagination = new PaginationManager({
        requestUrl: '/task/result',
        tableBodySelector: '#resultTableBody',
        paginationSelector: '#resultPagination',
        countSelector: '#totalCount',
        currentPageSelector: '#currentPage',
        totalPagesSelector: '#totalPages',
        pageSize: 10,
        params: {taskId: taskId},
        renderRow: function(result, index) {
            const sourceTableName = escapeHtml(result.sourceTableName || '-');
            const targetTableName = escapeHtml(result.targetTableName || '-');
            const updateTime = formatDateTime(result.updateTime);
            const resultContent = renderResultContent(result.content);
            
            // 将结果数据存储到全局映射表中
            if (result.id) {
                resultDataMap[result.id] = result;
            }
            
            // 创建行HTML
            const rowHtml = `
                <tr data-result-id="${result.id || ''}" class="result-row" style="cursor: pointer;">
                    <td class="text-center">${index}</td>
                    <td>
                        <div class="font-medium">${sourceTableName}</div>
                    </td>
                    <td>
                        <div class="font-medium">${targetTableName}</div>
                    </td>
                    <td>${resultContent}</td>
                    <td class="text-sm text-gray-500">${updateTime}</td>
                </tr>
            `;
            
            return rowHtml;
        },
        emptyHtml: `
            <tr>
                <td colspan="5" class="text-center">
                    <i class="fa fa-inbox empty-icon"></i>
                    <p class="empty-text">暂无执行结果</p>
                    <p class="empty-description">该任务还没有执行记录</p>
                </td>
            </tr>
        `,
        refreshCompleted: function() {
            bindResultEvents();
        }
    });
    
    // 保存分页管理器到全局作用域
    window.resultPagination = resultPagination;
    
    console.log('[任务执行结果] 分页管理器初始化完成');
}

/**
 * 绑定结果操作事件
 */
function bindResultEvents() {
    console.log('[任务执行结果] 绑定事件');
    
    // 查看详情按钮点击事件
    $('.view-detail-btn').off('click').on('click', function(e) {
        e.stopPropagation(); // 阻止事件冒泡到行
        
        const row = $(this).closest('tr');
        const resultId = row.data('result-id');
        
        console.log('[任务执行结果] 点击查看详情按钮, ID:', resultId);
        
        // 从全局映射表中获取数据
        const resultData = resultDataMap[resultId];
        
        if (resultData) {
            showDetailModal(resultData);
        } else {
            bootGrowl('无法获取详细数据', 'warning');
        }
    });
    
    // 行点击事件（整行都可点击查看详情）
    $('.result-row').off('click').on('click', function() {
        const viewBtn = $(this).find('.view-detail-btn');
        if (viewBtn.length > 0) {
            viewBtn.trigger('click');
        }
    });
}

// 页面加载完成后初始化
$(document).ready(function() {
    
    var taskId = $('#taskIdDisplay').text();
    console.log('taskId:'+taskId);
    
    // 初始化结果列表
    initResultList(taskId);
});

