/**
 * 通用分页管理器
 * @param {Object} options 配置选项
 *   - requestUrl: 请求地址（必需）
 *   - tableBodySelector: 表格体的选择器（必需）
 *   - paginationSelector: 分页区域的选择器（可选，不提供时会自动在 table 后面创建）
 *   - countSelector: 总数显示元素的选择器（可选，默认：.totalCount）
 *   - currentPageSelector: 当前页显示元素的选择器（可选，默认：.currentPage）
 *   - totalPagesSelector: 总页数显示元素的选择器（可选，默认：.totalPages）
 *   - renderRow: 自定义行渲染函数(item, index)（必需）
 *   - emptyHtml: 无数据时的HTML（可选）
 */
(function(window, $) {
    'use strict';
    
    // 依赖检查
    if (typeof window.doPoster !== 'function') {
        console.error('[PaginationManager] 依赖 doPoster 函数未找到');
        return;
    }
    if (typeof window.bootGrowl !== 'function') {
        console.error('[PaginationManager] 依赖 bootGrowl 函数未找到');
        return;
    }
    
    function PaginationManager(options) {
        const config = {
            requestUrl: options.requestUrl,
            tableBodySelector: options.tableBodySelector,
            paginationSelector: options.paginationSelector,
            countSelector: options.countSelector,
            currentPageSelector: options.currentPageSelector,
            totalPagesSelector: options.totalPagesSelector,
            renderRow: options.renderRow,
            emptyHtml: options.emptyHtml || '',
            params: options.params || {},
            pageIndex: options.pageIndex || 1,
            pageSize: options.pageSize || 10,
            refreshCompleted: options.refreshCompleted || function() {}
        };

        // 自动创建分页容器和结构
        this.initPaginationStructure = function() {
            let $pagination = config.paginationSelector ? $(config.paginationSelector) : $();
            let paginationId = '';
            
            // 如果分页容器不存在，自动在 table 后面创建
            if ($pagination.length === 0) {
                const $table = $(config.tableBodySelector).closest('table');
                if ($table.length === 0) {
                    console.error('[PaginationManager] 无法找到表格元素:', config.tableBodySelector);
                    return;
                }
                
                // 创建分页容器并插入到 table 后面
                paginationId = config.paginationSelector 
                    ? config.paginationSelector.replace('#', '') 
                    : 'pagination_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                $pagination = $('<div id="' + paginationId + '"></div>');
                $table.after($pagination);
                
                // 更新配置中的选择器
                config.paginationSelector = '#' + paginationId;
            } else {
                // 如果容器已存在，获取其 ID，如果没有则生成一个
                paginationId = $pagination.attr('id');
                if (!paginationId) {
                    paginationId = 'pagination_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
                    $pagination.attr('id', paginationId);
                    config.paginationSelector = '#' + paginationId;
                }
            }

            // 生成唯一的选择器前缀（基于分页容器 ID）
            const selectorPrefix = '#' + paginationId + ' ';
            
            // 如果没有指定自定义选择器，使用基于分页容器的唯一选择器
            config.countSelector = config.countSelector || selectorPrefix + '.totalCount';
            config.currentPageSelector = config.currentPageSelector || selectorPrefix + '.currentPage';
            config.totalPagesSelector = config.totalPagesSelector || selectorPrefix + '.totalPages';

            // 检查是否已有分页按钮容器，如果没有则自动创建完整结构
            const $paginationBar = $pagination.find('.pagination-bar');
            if ($paginationBar.length === 0) {
                // 如果没有样式类，添加默认样式
                if (!$pagination.hasClass('p-5')) {
                    $pagination.addClass('p-5 border-t border-gray-100 flex items-center justify-between');
                }
                
                // 检查是否已有分页信息文本
                const $paginationInfo = $pagination.find('.pagination-info');
                if ($paginationInfo.length === 0) {
                    // 创建分页信息文本
                    $pagination.prepend(`
                        <p class="text-sm text-gray-500 pagination-info">
                            共 <span class="totalCount">0</span> 条，第 <span class="currentPage">1</span> / <span class="totalPages">1</span> 页
                        </p>
                    `);
                }
                
                // 创建分页按钮容器
                $pagination.append('<div class="pagination-bar flex items-center gap-2"></div>');
            }
        };

        this.doSearch = function(params, pageNum) {
            params.pageNum = pageNum || config.pageIndex;
            params.pageSize = config.pageSize;
            const pagination = this;
            window.doPoster(config.requestUrl, params, function(data) {
                if (data.success === true) {
                    pagination.refreshPagination(data, params);
                } else {
                    window.bootGrowl('搜索异常，请重试', 'danger');
                }
            });
        };

        this.refreshPagination = function(response, params) {
            const result = response.data || {};
            const items = result.data || [];
            const total = result.total || 0;
            const pageNum = result.pageNum || config.pageIndex;
            // 更新分页管理器状态
            this.currentPage = pageNum;
            // 渲染表格
            this.renderTable(items);
            // 更新分页信息
            const totalPages = this.updateInfo(total, pageNum);

            // 渲染分页按钮
            this.renderPagination(pageNum, totalPages, (nextPage) => {
                this.doSearch(params, nextPage);
            });

            // 显示/隐藏分页区域
            this.togglePagination(items.length > 0);

            // 刷新完成
            config.refreshCompleted();
        };

        // 渲染表格数据
        this.renderTable = function(data) {
            const tbody = $(config.tableBodySelector);
            tbody.empty();
            if (!data || data.length === 0) {
                tbody.append(config.emptyHtml);
                return;
            }
            data.forEach((item, index) => {
                const i = (this.currentPage - 1) * config.pageSize + index + 1;
                const html = config.renderRow(item, i);
                tbody.append(html);
            });
        };

        // 渲染分页按钮
        this.renderPagination = function(currentPage, totalPages, onPageChange) {
            const pagination = $(config.paginationSelector);
            const paginationBar = $(config.paginationSelector).find(".pagination-bar");
            const paginationBtns = pagination.find('.pagination-btn');
            paginationBtns.remove();

            // 上一页按钮
            const prevBtn = $(`<button type="button" class="pagination-btn" ${currentPage === 1 ? 'disabled' : ''}>
                <i class="fa fa-angle-left"></i>
            </button>`);
            if (currentPage > 1) {
                prevBtn.on('click', () => onPageChange(currentPage - 1));
            }
            paginationBar.append(prevBtn);

            // 页码按钮（显示3个页码）
            const startPage = Math.max(1, currentPage - 1);
            const endPage = Math.min(totalPages, startPage + 2);

            for (let i = startPage; i <= endPage; i++) {
                const pageBtn = $(`<button type="button" class="pagination-btn ${i === currentPage ? 'active' : ''}">${i}</button>`);
                pageBtn.on('click', () => {
                    if (i !== currentPage) {
                        onPageChange(i);
                    }
                });
                paginationBar.append(pageBtn);
            }

            // 下一页按钮
            const nextBtn = $(`<button type="button" class="pagination-btn" ${currentPage === totalPages ? 'disabled' : ''}>
                <i class="fa fa-angle-right"></i>
            </button>`);
            if (currentPage < totalPages) {
                nextBtn.on('click', () => onPageChange(currentPage + 1));
            }
            paginationBar.append(nextBtn);
        };

        // 更新分页信息
        this.updateInfo = function(total, pageNo) {
            const totalPages = Math.ceil(total / config.pageSize) || 1;
            $(config.countSelector).text(total);
            $(config.currentPageSelector).text(pageNo);
            $(config.totalPagesSelector).text(totalPages);
            return totalPages;
        };

        // 显示/隐藏分页区域
        this.togglePagination = function(show) {
            $(config.paginationSelector).toggle(show);
        };

        // 初始化状态
        this.currentPage = config.pageIndex;
        
        // 初始化分页结构
        this.initPaginationStructure();
        
        // 开始加载数据
        this.doSearch(config.params, this.currentPage);
    }
    
    // 导出到全局
    window.PaginationManager = PaginationManager;
    
})(window, jQuery);

