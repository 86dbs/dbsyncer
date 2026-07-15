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
 *   - storageKey: localStorage 缓存键，持久化 pageNum、pageSize（可选）
 */
(function(window, $) {
    'use strict';

    const PAGINATION_STORAGE_PREFIX = 'dbsyncer.pagination.';

    function loadPaginationState(storageKey) {
        if (!storageKey) {
            return null;
        }
        try {
            const raw = localStorage.getItem(PAGINATION_STORAGE_PREFIX + storageKey);
            if (raw) {
                return JSON.parse(raw);
            }
        } catch (e) {
            console.warn('[PaginationManager] 读取分页缓存失败', e);
        }
        return null;
    }

    function savePaginationState(storageKey, pageNum, pageSize) {
        if (!storageKey) {
            return;
        }
        try {
            localStorage.setItem(PAGINATION_STORAGE_PREFIX + storageKey, JSON.stringify({
                pageNum: pageNum,
                pageSize: pageSize
            }));
        } catch (e) {
            console.warn('[PaginationManager] 保存分页缓存失败', e);
        }
    }
    
    // 依赖检查
    if (typeof window.doPoster !== 'function') {
        console.error('[PaginationManager] 依赖 doPoster 函数未找到');
        return;
    }
    if (typeof window.bootGrowl !== 'function') {
        console.error('[PaginationManager] 依赖 bootGrowl 函数未找到');
        return;
    }
    
    function createPaginationNavBtn(label, iconClass, disabled, onClick) {
        const btn = $(`<button type="button" class="pagination-btn pagination-btn-nav" title="${label}" ${disabled ? 'disabled' : ''}>
            <i class="fa ${iconClass}"></i>
        </button>`);
        if (!disabled && typeof onClick === 'function') {
            btn.on('click', onClick);
        }
        return btn;
    }

    function applyProgressFillWidths(container) {
        $(container).find('.progress-fill[data-progress-width]').each(function () {
            const width = this.getAttribute('data-progress-width');
            if (width !== null && width !== '') {
                this.style.setProperty('--progress-width', width + '%');
            }
        });
    }

    function getPageWindow(currentPage, totalPages, windowSize) {
        if (totalPages <= windowSize) {
            return { start: 1, end: totalPages };
        }
        let start = currentPage - Math.floor(windowSize / 2);
        let end = start + windowSize - 1;
        if (start < 1) {
            start = 1;
            end = windowSize;
        }
        if (end > totalPages) {
            end = totalPages;
            start = totalPages - windowSize + 1;
        }
        return { start: start, end: end };
    }

    function PaginationManager(options) {
        const storageKey = options.storageKey || '';
        const stored = loadPaginationState(storageKey);
        const defaultPageSize = options.pageSize || 5;
        const defaultPageIndex = options.pageIndex || 1;
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
            pageIndex: (stored && stored.pageNum) ? stored.pageNum : defaultPageIndex,
            pageSize: (stored && stored.pageSize) ? stored.pageSize : defaultPageSize,
            storageKey: storageKey,
            customPageSize: options.customPageSize || false,
            customPageSizeItems: options.customPageSizeItems ||  [5, 10, 50, 100, 200],
            showBoundaryButtons: options.showBoundaryButtons !== false,
            refreshCompleted: options.refreshCompleted || function() {}
        };

        const persistState = function(pageNum) {
            savePaginationState(config.storageKey, pageNum, config.pageSize);
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
                $pagination.append('<div class="pagination-bar flex items-center"></div>');
            }
        };

        this.doSearch = function(params, pageNum) {
            const searchParams = $.extend({}, this.lastSearchParams || {}, params || {});
            this.lastSearchParams = $.extend({}, searchParams);
            const targetPage = pageNum || this.currentPage || config.pageIndex;
            searchParams.pageNum = targetPage;
            searchParams.pageSize = config.pageSize;
            const pagination = this;
            window.doPoster(config.requestUrl, searchParams, function(data) {
                if (data.success === true) {
                    pagination.refreshPagination(data, searchParams);
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
            const totalPages = Math.ceil(total / config.pageSize) || 1;

            // 当前页超出有效范围时（如第 2 页删光数据），自动回到最后一页
            if (pageNum > totalPages) {
                this.doSearch(params, Math.max(1, totalPages));
                return;
            }

            // 更新分页管理器状态
            this.currentPage = pageNum;
            config.pageIndex = pageNum;
            this.total = total;
            persistState(pageNum);
            // 渲染表格
            this.renderTable(items);
            // 更新分页信息
            this.updateInfo(total, pageNum);

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
            applyProgressFillWidths(tbody);
        };

        // 渲染分页按钮
        this.renderPagination = function(currentPage, totalPages, onPageChange) {
            const pagination = $(config.paginationSelector);
            const paginationBar = $(config.paginationSelector).find(".pagination-bar");
            const paginationBtns = pagination.find('.pagination-btn, .pagination-page-btn');
            paginationBtns.remove();

            let $this = this;
            if (config.customPageSize) {
                // 检查下拉框是否已存在，如果存在则更新选中状态，不存在则创建
                let $sizeSelect = paginationBar.find('.page-size-select');
                if ($sizeSelect.length === 0) {
                    // 定义可选的每页条数
                    const currentSize = config.pageSize;
                    // 创建下拉框 HTML
                    let selectHtml = '<select class="page-size-select ml-2 mr-2 form-control-md">';
                    config.customPageSizeItems.forEach(size => {
                        const selected = size === currentSize ? 'selected' : '';
                        selectHtml += `<option value="${size}" ${selected}>${size}条/页</option>`;
                    });
                    selectHtml += '</select>';
                    const $sizeSelect = $(selectHtml);
                    paginationBar.append($sizeSelect);
                    $sizeSelect.dbSelect({
                        type: 'single',
                        onSelect: function (values) {
                            const selectedValue = values && values.length > 0 ? values[0] : '';
                            if (!isBlank(selectedValue)) {
                                const newSize = parseInt(selectedValue);
                                // 更新配置中的 pageSize
                                config.pageSize = newSize;
                                persistState(1);
                                // 重置到第一页并重新搜索
                                $this.doSearch({}, 1);
                            }
                        }
                    });
                } else {
                    // 定时刷新时仅同步显示，不可触发 onSelect（否则会 doSearch 死循环并重置到第 1 页）
                    const pageSizeApi = $sizeSelect.data('dbSelect');
                    const nextVal = String(config.pageSize);
                    const currentVal = pageSizeApi && typeof pageSizeApi.getValues === 'function'
                        ? String((pageSizeApi.getValues()[0] || ''))
                        : String($sizeSelect.val() || '');
                    if (currentVal !== nextVal) {
                        if (pageSizeApi && typeof pageSizeApi.setValues === 'function') {
                            pageSizeApi.setValues(nextVal, true);
                        } else {
                            $sizeSelect.val(config.pageSize);
                        }
                    }
                }
            }

            if (config.showBoundaryButtons) {
                // 首页按钮
                paginationBar.append(createPaginationNavBtn(
                    '首页',
                    'fa-angle-double-left',
                    currentPage === 1,
                    () => onPageChange(1)
                ));
            }

            // 上一页按钮
            paginationBar.append(createPaginationNavBtn(
                '上一页',
                'fa-angle-left',
                currentPage === 1,
                () => onPageChange(currentPage - 1)
            ));

            // 页码按钮：总页数 > 3 时固定展示 3 个，否则展示全部
            const pageWindow = getPageWindow(currentPage, totalPages, 3);
            for (let i = pageWindow.start; i <= pageWindow.end; i++) {
                const pageBtn = $(`<button type="button" class="pagination-page-btn ${i === currentPage ? 'active' : ''}">${i}</button>`);
                pageBtn.on('click', () => {
                    if (i !== currentPage) {
                        onPageChange(i);
                    }
                });
                paginationBar.append(pageBtn);
            }

            // 下一页按钮
            paginationBar.append(createPaginationNavBtn(
                '下一页',
                'fa-angle-right',
                currentPage === totalPages,
                () => onPageChange(currentPage + 1)
            ));

            if (config.showBoundaryButtons) {
                // 末页按钮
                paginationBar.append(createPaginationNavBtn(
                    '末页',
                    'fa-angle-double-right',
                    currentPage === totalPages,
                    () => onPageChange(totalPages)
                ));
            }
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
        this.total = 0;
        this.lastSearchParams = $.extend({}, config.params || {});
        
        // 初始化分页结构
        this.initPaginationStructure();
        
        // 开始加载数据
        this.doSearch(config.params, this.currentPage);
    }
    
    // 导出到全局
    window.PaginationManager = PaginationManager;
    
})(window, jQuery);

