/**
 * 通用分页管理器
 * @param {Object} options 配置选项
 *   - requestUrl: 请求地址（必需）
 *   - tableBodySelector: 表格体的选择器（必需）
 *   - paginationSelector: 分页区域的选择器（必需）
 *   - countSelector: 总数显示元素的选择器
 *   - currentPageSelector: 当前页显示元素的选择器
 *   - totalPagesSelector: 总页数显示元素的选择器
 *   - renderRow: 自定义行渲染函数(item, index, pageNo, pageSize)
 *   - emptyHtml: 无数据时的HTML
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
            countSelector: options.countSelector || '.totalCount',
            currentPageSelector: options.currentPageSelector || '.currentPage',
            totalPagesSelector: options.totalPagesSelector || '.totalPages',
            renderRow: options.renderRow,
            emptyHtml: options.emptyHtml || ''
        };

        this.doSearch = function(params, pageNum) {
            params.pageNum = pageNum || 1;
            const pagination = this;
            window.doPoster(config.requestUrl, params, function(data) {
                if (data.success === true) {
                    pagination.refreshPagination(data, params);
                } else {
                    window.bootGrowl('搜索异常，请重试', 'danger');
                }
            });
        };

        this.refreshPagination = function(data, params) {
            const resultValue = data.resultValue || {};
            const items = resultValue.data || [];
            const total = resultValue.total || 0;
            const pageNum = resultValue.pageNum || 1;
            const pageSize = resultValue.pageSize || 10;
            // 更新分页管理器状态
            this.currentPage = pageNum;
            this.pageSize = pageSize;
            // 渲染表格
            this.renderTable(items);
            // 更新分页信息
            const totalPages = this.updateInfo(total, pageNum, pageSize);

            // 渲染分页按钮
            this.renderPagination(pageNum, totalPages, (nextPage) => {
                this.doSearch(params, nextPage);
            });

            // 显示/隐藏分页区域
            this.togglePagination(items.length > 0);
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
                const i = (this.currentPage - 1) * this.pageSize + index + 1;
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
            const prevBtn = $(`<button class="pagination-btn" ${currentPage === 1 ? 'disabled' : ''}>
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
                const pageBtn = $(`<button class="pagination-btn ${i === currentPage ? 'active' : ''}">${i}</button>`);
                pageBtn.on('click', () => {
                    if (i !== currentPage) {
                        onPageChange(i);
                    }
                });
                paginationBar.append(pageBtn);
            }

            // 下一页按钮
            const nextBtn = $(`<button class="pagination-btn" ${currentPage === totalPages ? 'disabled' : ''}>
                <i class="fa fa-angle-right"></i>
            </button>`);
            if (currentPage < totalPages) {
                nextBtn.on('click', () => onPageChange(currentPage + 1));
            }
            paginationBar.append(nextBtn);
        };

        // 更新分页信息
        this.updateInfo = function(total, pageNo, pageSize) {
            const totalPages = Math.ceil(total / pageSize) || 1;
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
        this.currentPage = 1;
        this.pageSize = 10;
        this.doSearch({}, 1);
    }
    
    // 导出到全局
    window.PaginationManager = PaginationManager;
    
})(window, jQuery);

