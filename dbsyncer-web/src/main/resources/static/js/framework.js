// ******************* 初始化 *****************************
const $location = (window.location + '').split('/');
const $path = document.location.pathname;
const $basePath = $location[0] + '//' + $location[2] + $path.substr(0, $path.substr(1).indexOf("/") + 1);
const $mainContent = $('#mainContent');

// 工具函数
function showLoading() {
    $mainContent.html('<div class="loading"><div class="loading-spinner"></div>加载中...</div>');
}
function hideLoading() {
    $mainContent.find('.loading').remove();
}
function showEmpty(element, message) {
    $(element).html('<div class="empty"><div class="empty-icon"><i class="fa fa-inbox"></i></div><div class="empty-text">' + (message || '暂无数据') + '</div></div>');
}

// HTML转义
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatDate(time) {
    const date = new Date(time);
    const YY = date.getFullYear() + '-';
    const MM = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
    const DD = (date.getDate() < 10 ? '0' + (date.getDate()) : date.getDate());
    const hh = (date.getHours() < 10 ? '0' + date.getHours() : date.getHours()) + ':';
    const mm = (date.getMinutes() < 10 ? '0' + date.getMinutes() : date.getMinutes()) + ':';
    const ss = (date.getSeconds() < 10 ? '0' + date.getSeconds() : date.getSeconds());
    return YY + MM + DD + " " + hh + mm + ss;
}

/**
 * 判断字符串是否为空串
 * @eg undefined true
 * @eg null true
 * @eg '' true
 * @eg ' ' true
 * @eg '1' false
 * @return Boolean
 */
function isBlank(str) {
    return str === undefined || str === null || str === false || str.length === 0;
}

/**
 * 按照指定分隔符切分字符串
 *
 * @param str 带切分字符
 * @param delimiter 分隔符
 * @return Array
 */
function splitStrByDelimiter(str, delimiter) {
    return isBlank(str) ? [] : str.split(delimiter);
}

// ******************* 水印 ***************************
window.onresize = function () {
    watermark();
}
window.onscroll = function () {
    watermark();
}
//水印样式默认设置
const settings = {
    watermark_txt: "",
    watermark_x: 50,//水印起始位置x轴坐标
    watermark_y: 55,//水印起始位置Y轴坐标
    watermark_rows: 2000,//水印行数
    watermark_cols: 2000,//水印列数
    watermark_x_space: 70,//水印x轴间隔
    watermark_y_space: 30,//水印y轴间隔
    watermark_color: '#aaaaaa',//水印字体颜色
    watermark_alpha: 0.2,//水印透明度
    watermark_fontsize: '15px',//水印字体大小
    watermark_font: '微软雅黑',//水印字体
    watermark_width: 210,//水印宽度
    watermark_height: 80,//水印长度
    watermark_angle: 15//水印倾斜度数
};
let timestampWatermark;

function watermark() {
    const now = Date.now();
    if (timestampWatermark != null && now - timestampWatermark < 200) {
        return;
    }
    if (isBlank(settings.watermark_txt)) {
        return;
    }
    timestampWatermark = now;
    $(".dbsyncer_mask").remove();

    const water = document.body;
    //获取页面最大宽度
    const page_width = Math.max(water.scrollWidth, water.clientWidth);
    //获取页面最大高度
    const page_height = Math.max(water.scrollHeight, water.clientHeight);
    //水印数量自适应元素区域尺寸
    settings.watermark_cols = Math.ceil(page_width / (settings.watermark_x_space + settings.watermark_width));
    settings.watermark_rows = Math.ceil(page_height / (settings.watermark_y_space + settings.watermark_height));
    let x;
    let y;
    for (let i = 0; i < settings.watermark_rows; i++) {
        y = settings.watermark_y + (settings.watermark_y_space + settings.watermark_height) * i;
        for (let j = 0; j < settings.watermark_cols; j++) {
            x = settings.watermark_x + (settings.watermark_width + settings.watermark_x_space) * j;
            let mask_div = document.createElement('div');
            mask_div.className = 'dbsyncer_mask';
            mask_div.innerHTML = (settings.watermark_txt);
            mask_div.style.webkitTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.MozTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.msTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.OTransform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.transform = "rotate(-" + settings.watermark_angle + "deg)";
            mask_div.style.position = "absolute";
            mask_div.style.left = x + 'px';
            mask_div.style.top = y + 'px';
            mask_div.style.overflow = "hidden";
            mask_div.style.pointerEvents = 'none';
            mask_div.style.opacity = settings.watermark_alpha;
            mask_div.style.fontSize = settings.watermark_fontsize;
            mask_div.style.fontFamily = settings.watermark_font;
            mask_div.style.color = settings.watermark_color;
            mask_div.style.textAlign = "center";
            mask_div.style.width = settings.watermark_width + 'px';
            mask_div.style.height = settings.watermark_height + 'px';
            mask_div.style.display = "block";
            water.appendChild(mask_div);
        }
    }
}

// ******************* 扩展JS表单方法 ***************************
$.fn.serializeJson = function () {
    var o = {};
    var a = this.serializeArray();
    $.each(a, function () {
        if (o[this.name] !== undefined) {
            if (!o[this.name].push) {
                o[this.name] = [o[this.name]];
            }
            o[this.name].push(this.value || '');
        } else {
            o[this.name] = this.value || '';
        }
    });
    return o;
};

/**
 * 表单验证方法
 * 验证表单中所有带有 required 属性的字段
 * @returns {boolean} true-验证通过，false-验证失败
 */
$.fn.formValidate = function () {
    var $form = $(this);
    var isValid = true;
    var firstInvalidField = null;
    
    // 清除之前的错误提示
    $form.find('.form-error-message').remove();
    $form.find('.form-control-error').removeClass('form-control-error');
    
    // 查找所有必填字段
    $form.find('input[required], select[required], textarea[required]').each(function() {
        var $field = $(this);
        var value = $field.val();
        var fieldName = $field.attr('name');
        var fieldLabel = $field.attr('placeholder') || fieldName || '此字段';
        
        // 跳过隐藏的字段
        if ($field.is(':hidden') || $field.closest('.hidden').length > 0) {
            return true; // continue
        }
        
        // 检查值是否为空
        var isEmpty = false;
        if (value === null || value === undefined || value === '') {
            isEmpty = true;
        } else if (Array.isArray(value) && value.length === 0) {
            isEmpty = true;
        } else if (typeof value === 'string' && value.trim() === '') {
            isEmpty = true;
        }
        
        if (isEmpty) {
            isValid = false;
            
            // 标记字段为错误状态
            $field.addClass('form-control-error');
            
            // 添加错误提示（如果还没有）
            if ($field.next('.form-error-message').length === 0) {
                var errorMsg = '请填写' + fieldLabel;
                if ($field.is('select')) {
                    errorMsg = '请选择' + fieldLabel;
                }
                $field.after('<div class="form-error-message">' + errorMsg + '</div>');
            }
            
            // 记录第一个无效字段
            if (!firstInvalidField) {
                firstInvalidField = $field;
            }
        }
    });
    
    // 如果验证失败，聚焦到第一个无效字段并显示提示
    if (!isValid && firstInvalidField) {
        // 滚动到第一个错误字段
        $('html, body').animate({
            scrollTop: firstInvalidField.offset().top - 100
        }, 300);
        
        // 聚焦字段
        firstInvalidField.focus();
        
        // 显示全局提示
        if (typeof bootGrowl === 'function') {
            bootGrowl('请填写完整表单信息', 'danger');
        }
    }
    
    return isValid;
};

// 全局加载页面
function doLoader(url) {
    // 使用统一的内容区域
    if ($mainContent.length) {
        // 显示加载状态
        showLoading();

        // 加载页面内容
        $mainContent.load($basePath + url, function (response, status, xhr) {
            hideLoading();
            if (status !== 'success') {
                bootGrowl('页面加载失败，请稍后重试', "danger");
                return;
            }
        });
    }
}

// 异常请求
function doRequest(action, data) {
    hideLoading();
    // 异常请求：302
    if (!(data instanceof Object)) {
        bootGrowl("会话过期, 3秒后将访问登录主页...", "danger");
        setTimeout(function () {
            location.href = $basePath;
        }, 3000);
    } else {
        action(data);
    }
}

// 异常响应
function doErrorResponse(xhr, status, info) {
    hideLoading();
    bootGrowl("访问异常，请刷新或重试.", "danger");
}

// 全局Ajax post
function doPoster(url, params, action) {
    $.post($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

// 全局Ajax get
function doGetter(url, params, action) {
    $.get($basePath + url, params, function (data) {
        doRequest(action, data);
    }).error(function (xhr, status, info) {
        doErrorResponse(xhr, status, info);
    });
}

/**
 * 初始化标签输入框（Tagsinput）
 * 将带有 data-role="tagsinput" 的输入框转换为标签输入框
 * 支持添加/删除标签，提交时用逗号拼接
 */
function initMultipleInputTags() {
    const inputs = document.querySelectorAll('input[data-role="tagsinput"]');
    
    inputs.forEach(function(input) {
        // 如果已经初始化过，跳过
        if (input.classList.contains('tagsinput-initialized')) {
            return;
        }
        
        // 标记为已初始化
        input.classList.add('tagsinput-initialized');
        
        // 创建标签容器
        const container = document.createElement('div');
        container.className = 'tagsinput';
        
        // 创建标签列表容器
        const tagsList = document.createElement('div');
        tagsList.className = 'tagsinput-tags';
        
        // 创建新标签输入框
        const newTagInput = document.createElement('input');
        newTagInput.type = 'text';
        newTagInput.className = 'tagsinput-input';
        newTagInput.placeholder = input.placeholder || '输入后按 Enter 添加';
        
        // 隐藏原始输入框
        input.style.display = 'none';
        
        // 插入容器到 DOM
        input.parentNode.insertBefore(container, input);
        container.appendChild(tagsList);
        container.appendChild(newTagInput);
        
        // 存储标签值的数组
        let tags = [];
        
        // 初始化已有的值
        const initialValue = input.value || input.getAttribute('value');
        if (initialValue) {
            const initialTags = initialValue.split(',').map(function(tag) {
                return tag.trim();
            }).filter(function(tag) {
                return tag !== '';
            });
            initialTags.forEach(function(tag) {
                addTag(tag);
            });
        }
        
        // 添加标签
        function addTag(value) {
            value = value.trim();
            if (!value) return;
            
            // 检查是否已存在
            if (tags.indexOf(value) !== -1) {
                return;
            }
            
            tags.push(value);
            
            // 创建标签元素
            const tagElement = document.createElement('span');
            tagElement.className = 'tag';
            tagElement.innerHTML = `
                <span class="tag-text">${escapeHtml(value)}</span>
                <i class="tag-remove fa fa-times"></i>
            `;
            
            // 绑定删除事件
            tagElement.querySelector('.tag-remove').addEventListener('click', function() {
                removeTag(value, tagElement);
            });
            
            tagsList.appendChild(tagElement);
            updateInputValue();
        }
        
        // 删除标签
        function removeTag(value, element) {
            const index = tags.indexOf(value);
            if (index > -1) {
                tags.splice(index, 1);
            }
            element.remove();
            updateInputValue();
        }
        
        // 更新原始输入框的值
        function updateInputValue() {
            input.value = tags.join(',');
        }
        
        // HTML 转义
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // 监听新标签输入框的键盘事件
        newTagInput.addEventListener('keydown', function(e) {
            if (e.key === 'Enter' || e.key === ',') {
                e.preventDefault();
                const value = newTagInput.value.trim();
                if (value) {
                    addTag(value);
                }
                // 无论是否成功添加，都清空输入框
                newTagInput.value = '';
            } else if (e.key === 'Backspace' && !newTagInput.value && tags.length > 0) {
                // 如果输入框为空且按下退格键，删除最后一个标签
                const lastTag = tags[tags.length - 1];
                const lastTagElement = tagsList.lastElementChild;
                removeTag(lastTag, lastTagElement);
            }
        });
        
        // 监听失焦事件
        newTagInput.addEventListener('blur', function() {
            const value = newTagInput.value.trim();
            if (value) {
                addTag(value);
            }
            // 失焦时总是清空输入框
            newTagInput.value = '';
        });
        
        // 点击容器聚焦到输入框
        container.addEventListener('click', function() {
            newTagInput.focus();
        });
    });
}

/**
 * 初始化二维码悬浮提示
 * @param {Object} options - 配置选项
 * @param {string} options.url - 二维码链接地址
 * @param {string} options.selector - 目标元素选择器（多个用逗号分隔）
 * @param {number} options.size - 二维码大小，默认 150
 * @param {string} options.position - 位置，默认 'bottom'（bottom/top/left/right）
 */
function initQRCodePopover(options) {
    const config = {
        url: options.url || '',
        selector: options.selector || '',
        size: options.size || 150,
        position: options.position || 'bottom'
    };

    if (!config.url || !config.selector) {
        console.warn('initQRCodePopover: url and selector are required');
        return;
    }

    // 创建隐藏的二维码容器
    let qrcodeContainer = document.getElementById('qrcode-temp-container');
    if (!qrcodeContainer) {
        qrcodeContainer = document.createElement('div');
        qrcodeContainer.id = 'qrcode-temp-container';
        qrcodeContainer.style.cssText = 'position: absolute; left: -9999px; top: -9999px;';
        document.body.appendChild(qrcodeContainer);
    }

    // 生成二维码
    qrcodeContainer.innerHTML = ''; // 清空
    if (typeof QRCode !== 'undefined') {
        new QRCode(qrcodeContainer, {
            text: config.url,
            width: config.size,
            height: config.size,
            colorDark: '#000000',
            colorLight: '#ffffff',
            correctLevel: QRCode.CorrectLevel.H
        });
    } else {
        console.error('QRCode library not found');
        return;
    }

    // 等待二维码生成
    setTimeout(function() {
        const qrcodeImg = qrcodeContainer.querySelector('img');
        if (!qrcodeImg) {
            console.error('QRCode image not generated');
            return;
        }

        const qrcodeDataUrl = qrcodeImg.src;

        // 为所有匹配的元素添加悬浮效果
        const elements = document.querySelectorAll(config.selector);
        elements.forEach(function(element) {
            // 创建悬浮提示
            const popover = document.createElement('div');
            popover.className = 'qrcode-popover';
            popover.innerHTML = `
                <div class="qrcode-arrow"></div>
                <div class="qrcode-inner">
                    <img src="${qrcodeDataUrl}" alt="微信扫码" />
                    <div class="qrcode-text text-center text-success">
                        <i class="fa fa-wechat"></i> 微信扫码联系
                    </div>
                </div>
            `;
            document.body.appendChild(popover);

            let showTimer = null;
            let hideTimer = null;

            // 显示二维码
            function showQRCode() {
                clearTimeout(hideTimer);
                showTimer = setTimeout(function() {
                    const rect = element.getBoundingClientRect();
                    const popoverRect = popover.getBoundingClientRect();
                    
                    let top, left;
                    popover.className = 'qrcode-popover qrcode-' + config.position;

                    switch (config.position) {
                        case 'top':
                            top = rect.top - popoverRect.height - 10;
                            left = rect.left + (rect.width - popoverRect.width) / 2;
                            break;
                        case 'left':
                            top = rect.top + (rect.height - popoverRect.height) / 2;
                            left = rect.left - popoverRect.width - 10;
                            break;
                        case 'right':
                            top = rect.top + (rect.height - popoverRect.height) / 2;
                            left = rect.right + 10;
                            break;
                        case 'bottom':
                        default:
                            top = rect.bottom + 10;
                            left = rect.left + (rect.width - popoverRect.width) / 2;
                            break;
                    }

                    popover.style.top = top + window.scrollY + 'px';
                    popover.style.left = left + window.scrollX + 'px';
                    popover.classList.add('show');
                }, 300); // 延迟 300ms 显示
            }

            // 隐藏二维码
            function hideQRCode() {
                clearTimeout(showTimer);
                hideTimer = setTimeout(function() {
                    popover.classList.remove('show');
                }, 200); // 延迟 200ms 隐藏
            }

            // 绑定事件
            element.addEventListener('mouseenter', showQRCode);
            element.addEventListener('mouseleave', hideQRCode);
            popover.addEventListener('mouseenter', function() {
                clearTimeout(hideTimer);
            });
            popover.addEventListener('mouseleave', hideQRCode);
        });
    }, 500);
}

function initSearch(searchWrapperId, callback) {
    const wrapper = document.getElementById(searchWrapperId);
    const searchInput = wrapper.querySelector('.search-input');
    const searchClear = wrapper.querySelector('.search-clear');

    if (!searchInput || !searchClear || searchInput.dataset.selectBound === 'true') {
        return;
    }

    // 更新搜索状态（显示/隐藏清除按钮）
    function updateSearchState(keyword) {
        if (keyword && keyword.trim() !== '') {
            searchClear.classList.add('active');
        } else {
            searchClear.classList.remove('active');
        }
    }

    // 绑定事件
    searchInput.addEventListener('input', function(e) {
        const value = e.target.value;
        // 立即更新清除按钮显示状态
        updateSearchState(value);
    });

    // 清除搜索
    searchClear.addEventListener('click', function () {
        searchInput.value = '';
        searchInput.focus();
        updateSearchState('');
        callback(searchInput.value);
    });

    // 回车键搜索
    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            updateSearchState(e.target.value);
            callback(e.target.value);
        }
    });

    // 避免重复绑定
    searchInput.dataset.selectBound = 'true';

    // 初始化状态
    updateSearchState(searchInput.value || '');
}

/**
 * 通用分页
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
function PaginationManager(options) {
    const config = {
        requestUrl:options.requestUrl,
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
        doPoster(config.requestUrl, params, function(data) {
            if (data.success === true) {
                pagination.refreshPagination(data, params);
            } else {
                bootGrowl('搜索异常，请重试', 'danger');
            }
        });
    }

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
            this.doSearch(params, nextPage)
        });

        // 显示/隐藏分页区域
        this.togglePagination(items.length > 0);
    }

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

/**
 * jQuery Select 组件插件
 * @param {Object} options - 配置选项
 *   - type: 'single' 或 'multiple'（默认 'single'）
 *   - data: 选项数据数组 [{label: '', value: ''}, ...]
 *   - defaultValue: 默认选中值
 *   - disabled: 是否禁用
 *   - customButtons: 自定义按钮数组，最多2个
 *     [{text: '按钮文本', callback: function() {}}]
 *   - onSelect: 选择变化时的回调
 *   - onCustomButton: 自定义按钮点击时的回调
 */
$.fn.dbSelect = function(options) {
    options = options || {};
    
    const $select = $(this);
    if (!$select.length) return $select;
    
    // 防止重复初始化
    if ($select.data('dbSelectInitialized')) {
        return $select;
    }
    $select.data('dbSelectInitialized', true);

    // 默认配置
    const config = {
        type: options.type || 'single',
        data: options.data || [],
        defaultValue: options.defaultValue || null,
        disabled: options.disabled || false,
        customButtons: (options.customButtons || []).slice(0, 2), // 最多2个
        onSelect: options.onSelect || function() {},
        onCustomButton: options.onCustomButton || function() {}
    };

    // 从 HTML 中读取选项
    if ($select.is('select') && config.data.length === 0) {
        config.data = [];
        $select.find('option').each(function() {
            config.data.push({
                label: $(this).text(),
                value: $(this).val(),
                disabled: $(this).prop('disabled')
            });
        });
    }

    // 创建 Select 组件 HTML
    const selectId = 'dbsyncer-select-' + Date.now();
    const dropdownId = 'dbsyncer-select-dropdown-' + Date.now();
    
    const selectHTML = `
        <div class="dbsyncer-select ${config.disabled ? 'disabled' : ''}" id="${selectId}">
            <div class="dbsyncer-select-trigger" data-toggle>
                <div class="dbsyncer-select-trigger-content">
                    <span class="dbsyncer-select-trigger-text">${config.type === 'single' ? '请选择' : '请选择'}</span>
                    <div class="dbsyncer-select-trigger-tags"></div>
                </div>
                <div class="dbsyncer-select-arrow">
                    <i class="fa fa-angle-down"></i>
                </div>
            </div>
            <div class="dbsyncer-select-dropdown hidden" id="${dropdownId}">
                <div class="dbsyncer-select-search-wrapper">
                    <div class="dbsyncer-select-search">
                        <i class="fa fa-search"></i>
                        <input type="text" class="dbsyncer-select-search-input" placeholder="搜索...">
                        <div class="dbsyncer-select-search-clear"><i class="fa fa-times"></i></div>
                    </div>
                </div>
                <div class="dbsyncer-select-actions"></div>
                <div class="dbsyncer-select-options"></div>
                <div class="dbsyncer-select-empty hidden">暂无数据</div>
            </div>
        </div>
    `;

    $select.hide();
    // 在原有元素后添加自定义组件
    $select.after(selectHTML);
    const $component = $('#' + selectId);
    const $trigger = $component.find('.dbsyncer-select-trigger');
    const $dropdown = $component.find('.dbsyncer-select-dropdown');
    const $searchInput = $component.find('.dbsyncer-select-search-input');
    const $searchClear = $component.find('.dbsyncer-select-search-clear');
    const $options = $component.find('.dbsyncer-select-options');
    const $actions = $component.find('.dbsyncer-select-actions');
    const $empty = $component.find('.dbsyncer-select-empty');
    const $tags = $component.find('.dbsyncer-select-trigger-tags');
    const $text = $component.find('.dbsyncer-select-trigger-text');

    // 已选值
    let selectedValues = config.defaultValue ? (Array.isArray(config.defaultValue) ? config.defaultValue : [config.defaultValue]) : [];

    // 渲染选项
    function renderOptions(filterText = '') {
        $options.empty();
        const filterLower = filterText.toLowerCase();
        let hasVisible = false;

        config.data.forEach(function(item, index) {
            const matches = !filterText || item.label.toLowerCase().indexOf(filterLower) > -1;
            if (matches) {
                hasVisible = true;
            }

            const isSelected = selectedValues.indexOf(item.value) > -1;
            const inputType = config.type === 'single' ? 'radio' : 'checkbox';
            const inputName = selectId + '-' + inputType;

            // 单选时隐藏复选框，多选时显示
            const checkboxClass = config.type === 'single' ? 'dbsyncer-select-option-checkbox hidden' : 'dbsyncer-select-option-checkbox';

            const $option = $(`
                <label class="dbsyncer-select-option ${matches ? '' : 'hidden'} ${isSelected ? 'selected' : ''} ${item.disabled ? 'disabled' : ''}">
                    <input type="${inputType}" name="${inputName}" value="${escapeAttr(item.value)}" ${isSelected ? 'checked' : ''} ${item.disabled ? 'disabled' : ''}>
                    <div class="${checkboxClass}"></div>
                    <span class="dbsyncer-select-option-text">${escapeHtml(item.label)}</span>
                </label>
            `);

            $option.on('click', function(e) {
                if (item.disabled) {
                    e.preventDefault();
                    return;
                }
                e.preventDefault();
                handleOptionChange($option, item.value, item.label);
            });

            $options.append($option);
        });

        $empty.toggleClass('hidden', hasVisible);
    }

    // 处理选项变化
    function handleOptionChange($option, value, label) {
        if (config.type === 'single') {
            selectedValues = [value];
            $options.find('label').removeClass('selected');
            $option.addClass('selected');
            // 单选时自动关闭下拉菜单
            closeDropdown();
        } else {
            const index = selectedValues.indexOf(value);
            if (index > -1) {
                selectedValues.splice(index, 1);
                $option.removeClass('selected');
            } else {
                selectedValues.push(value);
                $option.addClass('selected');
            }
        }

        updateDisplay();
        triggerSelectEvent();
    }

    // 更新显示
    function updateDisplay() {
        // 更新原生 select
        if ($select.is('select')) {
            $select.val(config.type === 'single' ? (selectedValues[0] || '') : selectedValues);
        }

        // 更新标签显示
        $tags.empty();

        if (selectedValues.length > 0) {
            const labels = selectedValues.map(function(value) {
                const item = config.data.find(d => d.value === value);
                return item ? item.label : value;
            });

            if (config.type === 'single') {
                $text.text(labels[0]).show();
            } else {
                // 多选：显示前3个标签 + 计数
                $text.hide();
                const displayCount = 3;
                if (labels.length <= displayCount) {
                    labels.forEach(function(label) {
                        const $tag = $(`<span class="dbsyncer-select-tag">${escapeHtml(label)}<i class="fa fa-times dbsyncer-select-tag-remove"></i></span>`);
                        $tag.find('.dbsyncer-select-tag-remove').on('click', function(e) {
                            e.stopPropagation();
                            const idx = selectedValues.indexOf(config.data.find(d => d.label === label).value);
                            if (idx > -1) {
                                selectedValues.splice(idx, 1);
                                updateDisplay();
                                triggerSelectEvent();
                                renderOptions($searchInput.val());
                            }
                        });
                        $tags.append($tag);
                    });
                } else {
                    for (let i = 0; i < displayCount; i++) {
                        const label = labels[i];
                        const $tag = $(`<span class="dbsyncer-select-tag">${escapeHtml(label)}<i class="fa fa-times dbsyncer-select-tag-remove"></i></span>`);
                        $tag.find('.dbsyncer-select-tag-remove').on('click', function(e) {
                            e.stopPropagation();
                            const idx = selectedValues.indexOf(config.data.find(d => d.label === label).value);
                            if (idx > -1) {
                                selectedValues.splice(idx, 1);
                                updateDisplay();
                                triggerSelectEvent();
                                renderOptions($searchInput.val());
                            }
                        });
                        $tags.append($tag);
                    }
                    const count = labels.length - displayCount;
                    $tags.append(`<span class="dbsyncer-select-count">+${count}</span>`);
                }
            }
        } else {
            // 没有选中任何值时显示提示文字
            $text.text('请选择').show();
        }
    }

    // 触发选择事件
    function triggerSelectEvent() {
        // 回调函数
        if (typeof config.onSelect === 'function') {
            config.onSelect(selectedValues, config.type);
        }
        // 触发自定义事件
        $component.trigger('dbselect:change', [selectedValues]);
    }

    // 打开下拉菜单
    function openDropdown() {
        if (!config.disabled) {
            $component.addClass('open');
            $dropdown.removeClass('hidden');
            $searchInput.focus();
            renderOptions('');
        }
    }

    // 关闭下拉菜单
    function closeDropdown() {
        $component.removeClass('open');
        $dropdown.addClass('hidden');
        $searchInput.val('');
        $searchClear.removeClass('active');
        renderOptions('');
    }

    // 全选
    function selectAll() {
        if (config.type === 'multiple') {
            config.data.forEach(function(item) {
                if (!item.disabled && selectedValues.indexOf(item.value) === -1) {
                    selectedValues.push(item.value);
                }
            });
            updateDisplay();
            triggerSelectEvent();
            renderOptions($searchInput.val());
        }
    }

    // 取消全选
    function deselectAll() {
        selectedValues = [];
        updateDisplay();
        triggerSelectEvent();
        renderOptions($searchInput.val());
    }

    // HTML转义
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // 属性转义
    function escapeAttr(text) {
        return String(text).replace(/[&<>"']/g, function(s) {
            return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[s];
        });
    }

    // 绑定事件
    $trigger.on('click', function(e) {
        e.stopPropagation();
        if ($dropdown.hasClass('hidden')) {
            openDropdown();
        } else {
            closeDropdown();
        }
    });

    // 搜索
    $searchInput.on('input', function() {
        const text = $(this).val();
        $searchClear.toggleClass('active', text.length > 0);
        renderOptions(text);
    });

    // 清除搜索
    $searchClear.on('click', function(e) {
        e.stopPropagation();
        $searchInput.val('').focus();
        $searchClear.removeClass('active');
        renderOptions('');
    });

    // 点击外部关闭
    $(document).on('click.dbselect_' + selectId, function(e) {
        if (!$component.is(e.target) && $component.has(e.target).length === 0) {
            closeDropdown();
        }
    });

    // 操作按钮
    if (config.type === 'multiple') {
        // 添加默认按钮
        const $selectAllBtn = $(`<button class="dbsyncer-select-action-btn">全选</button>`);
        const $deselectAllBtn = $(`<button class="dbsyncer-select-action-btn">取消全选</button>`);

        $selectAllBtn.on('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            selectAll();
        });

        $deselectAllBtn.on('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            deselectAll();
        });

        $actions.append($selectAllBtn).append($deselectAllBtn);

        // 添加自定义按钮
        config.customButtons.forEach(function(btn, index) {
            const $btn = $(`<button class="dbsyncer-select-action-btn">${escapeHtml(btn.text)}</button>`);
            $btn.on('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (typeof btn.callback === 'function') {
                    btn.callback(selectedValues);
                }
                // 触发自定义事件
                $component.trigger('dbselect:button', [index, selectedValues]);
                // 调用全局回调
                if (typeof config.onCustomButton === 'function') {
                    config.onCustomButton(index, selectedValues, btn.text);
                }
            });
            $actions.append($btn);
        });
    }

    // 初始化显示
    renderOptions('');
    updateDisplay();

    // 如果没有设置默认值，检查是否有 selected 属性或默认选中第一个
    if (selectedValues.length === 0 && $select.is('select')) {
        // 检查是否有 selected 属性的选项
        const $selectedOption = $select.find('option:selected');
        if ($selectedOption.length && $selectedOption.val()) {
            selectedValues = [$selectedOption.val()];
        } else {
            // 没有 selected 属性，默认选中第一个非空选项
            const $firstOption = $select.find('option').filter(function() {
                return $(this).val() && $(this).val() !== '';
            }).first();
            if ($firstOption.length) {
                selectedValues = [$firstOption.val()];
            }
        }
        // 重新更新显示
        if (selectedValues.length > 0) {
            updateDisplay();
            renderOptions('');
            triggerSelectEvent();
        }
    }

    // 保存配置
    $component.data('dbSelect', {
        getValues: function() { return selectedValues; },
        setValues: function(values) {
            selectedValues = Array.isArray(values) ? values : [values];
            updateDisplay();
            renderOptions($searchInput.val());
            triggerSelectEvent();
        },
        clear: function() {
            selectedValues = [];
            updateDisplay();
            renderOptions('');
            triggerSelectEvent();
        },
        destroy: function() {
            $(document).off('click.dbselect_' + selectId);
            $component.remove();
        }
    });

    return $select;
};

/**
 * 确认对话框组件
 * @param {Object} options - 配置选项
 *   - title: 对话框标题（必需）
 *   - message: 对话框内容信息
 *   - body: 对话框主体HTML内容（可选）
 *   - icon: 图标类型（info/warning/error/success，默认 info）
 *   - confirmText: 确认按钮文本（默认 '确定'）
 *   - cancelText: 取消按钮文本（默认 '取消'）
 *   - confirmType: 确认按钮类型（primary/success/warning/danger/info，默认 primary）
 *   - size: 对话框大小（normal/large/max，默认 normal）
 *   - onConfirm: 确认回调函数
 *   - onCancel: 取消回调函数（可选）
 */
function showConfirm(options) {
    options = options || {};
    
    // 参数验证
    if (!options.title) {
        console.warn('[showConfirm] 缺少必要参数: title');
        return;
    }

    // 默认配置
    const config = {
        title: options.title || '',
        message: options.message || '',
        body: options.body || '',
        icon: options.icon || 'info',
        confirmText: options.confirmText || '确定',
        cancelText: options.cancelText || '取消',
        confirmType: options.confirmType || 'primary',
        size: options.size || 'normal',
        onConfirm: options.onConfirm || function() {},
        onCancel: options.onCancel || function() {}
    };

    // 验证icon类型
    const validIcons = ['info', 'warning', 'error', 'success'];
    if (validIcons.indexOf(config.icon) === -1) {
        config.icon = 'info';
    }

    // 验证size
    const validSizes = ['normal', 'large', 'max'];
    if (validSizes.indexOf(config.size) === -1) {
        config.size = 'normal';
    }

    // 验证确认按钮类型
    const validTypes = ['primary', 'success', 'warning', 'danger', 'info'];
    if (validTypes.indexOf(config.confirmType) === -1) {
        config.confirmType = 'primary';
    }

    // 获取icon HTML
    function getIconHTML() {
        const iconMap = {
            'info': '<i class="fa fa-info-circle"></i>',
            'warning': '<i class="fa fa-exclamation-triangle"></i>',
            'error': '<i class="fa fa-times-circle"></i>',
            'success': '<i class="fa fa-check-circle"></i>'
        };
        return iconMap[config.icon] || iconMap['info'];
    }

    // 创建HTML
    const overlayId = 'confirm-overlay-' + Date.now();
    const dialogId = 'confirm-dialog-' + Date.now();
    
    const overlay = document.createElement('div');
    overlay.id = overlayId;
    overlay.className = 'confirm-overlay';

    const dialog = document.createElement('div');
    dialog.id = dialogId;
    dialog.className = 'confirm-dialog size-' + config.size;

    // 构建头部HTML
    let headerHTML = '';
    if (config.title || config.message) {
        headerHTML = `
            <div class="confirm-header">
                <div class="confirm-icon icon-${config.icon}">
                    ${getIconHTML()}
                </div>
                <div class="confirm-content-wrapper">
                    ${config.title ? `<h3 class="confirm-title">${escapeHtml(config.title)}</h3>` : ''}
                    ${config.message ? `<p class="confirm-message">${escapeHtml(config.message)}</p>` : ''}
                </div>
            </div>
        `;
    }

    // 构建主体HTML
    let bodyHTML = '';
    if (config.body) {
        bodyHTML = `<div class="confirm-body">${config.body}</div>`;
    }

    dialog.innerHTML = `
        ${headerHTML}
        ${bodyHTML}
        <div class="confirm-footer">
            <button id="confirm-btn-confirm-${Date.now()}" class="btn confirm-btn confirm-btn-type-${config.confirmType}">
                ${escapeHtml(config.confirmText)}
            </button>
            <button id="confirm-btn-cancel-${Date.now()}" class="btn confirm-btn confirm-btn-cancel">
                ${escapeHtml(config.cancelText)}
            </button>
        </div>
    `;

    overlay.appendChild(dialog);
    document.body.appendChild(overlay);

    // HTML转义函数
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // 获取按钮
    const confirmBtn = dialog.querySelector('button:first-child');
    const cancelBtn = dialog.querySelector('button:last-child');

    // 关闭对话框
    function closeConfirm() {
        dialog.classList.add('hidden');
        overlay.classList.add('hidden');
        setTimeout(function() {
            if (overlay.parentNode) {
                overlay.parentNode.removeChild(overlay);
            }
        }, 200);
    }

    // 确认按钮事件
    confirmBtn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        closeConfirm();
        if (typeof config.onConfirm === 'function') {
            config.onConfirm();
        }
    });

    // 取消按钮事件
    cancelBtn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        closeConfirm();
        if (typeof config.onCancel === 'function') {
            config.onCancel();
        }
    });

    // 点击非对话框区域关闭
    overlay.addEventListener('click', function(e) {
        if (e.target === overlay) {
            e.preventDefault();
            closeConfirm();
            if (typeof config.onCancel === 'function') {
                config.onCancel();
            }
        }
    });

    // ESC键关闭（可选）
    function handleEsc(e) {
        if (e.key === 'Escape') {
            closeConfirm();
            if (typeof config.onCancel === 'function') {
                config.onCancel();
            }
            document.removeEventListener('keydown', handleEsc);
        }
    }
    document.addEventListener('keydown', handleEsc);

    return {
        close: closeConfirm,
        getDialog: function() { return dialog; },
        getOverlay: function() { return overlay; }
    };
}

// 注销
function logout() {
    // 基础用法
    showConfirm({
        title: '确定要注销？',
        message: '确定后将跳转至登录页面',
        icon: 'info',
        size: 'large',
        confirmType: 'info',
        onConfirm: function () {
            doPoster("/logout", null, function (data) {
                location.href = $basePath;
            });
        }
    });
}

$(function () {
    // 定义返回函数，子页面返回
    window.backIndexPage = function () {
        doLoader('/index');
    };

    // 导出到全局
    window.DBSyncerTheme = {
        showLoading: showLoading,
        hideLoading: hideLoading,
        showEmpty: showEmpty,
        formatDate: formatDate,
        validateForm: validateForm,
        notify: notify,
        initFileUpload: initFileUpload,
        initQRCodePopover: initQRCodePopover,
        initMultipleInputTags: initMultipleInputTags,
        initSearch: initSearch,
        showConfirm: showConfirm
    };

    // 刷新授权信息
    doGetter("/license/query.json", {}, function (data) {
        if (data.success === true) {
            // 社区版
            if (isBlank(data.resultValue.key)) {
                return;
            }
            $("#licenseInfo").show();
            // 专业版
            const licenseInfo = data.resultValue;
            const $content = $("#effectiveContent");
            const $effectiveTime = licenseInfo.effectiveTime;
            if ($effectiveTime <= 0) {
                $content.text('未激活');
                $content.addClass('text-warning');
                return;
            }
            const $currentTime = licenseInfo.currentTime;
            const $10days = 864000000;
            // 有效期内
            if ($currentTime < $effectiveTime && $effectiveTime - $10days > $currentTime) {
                $("#licenseCheck").removeClass("hidden");
            }
            // 即将过期
            else if ($currentTime < $effectiveTime && $effectiveTime - $10days <= $currentTime) {
                $("#licenseRemind").removeClass("hidden");
            }
            // 已过期
            else if ($currentTime > $effectiveTime) {
                $("#licenseWarning").removeClass("hidden");
            }
            $content.text(licenseInfo.effectiveContent);
        }
    });

    // 刷新登录用户
    doGetter("/user/getUserInfo.json", {}, function (data) {
        if (data.success === true) {
            $("#currentUser").text(data.resultValue.nickname);
        }
    });

    // 初始化版权信息
    doGetter("/index/version.json", {}, function (data) {
        if (data.success === true) {
            // 获取底部版权信息
            $("#copyRight").html(data.resultValue.appCopyRight);
            settings.watermark_txt = data.resultValue.watermark;
            watermark();
        }
    });

    // 新导航链接点击事件
    $('.sidebar-item[url]').on('click', function(e) {
        e.preventDefault();
        // 更新活动状态
        $('.sidebar-item').removeClass('active');
        $(this).addClass('active');
        // 加载页面
        doLoader($(this).attr('url'));
    });

    // 下拉菜单
    const $dropdown = $(".dropdown");
    const $dropdownBtn = $(".dropdown button");
    const $dropdownMenu = $(".dropdown .dropdown-menu");
    $dropdownBtn.on('click', function (){
        event.stopPropagation();
        // 只切换当前下拉菜单，不影响其他下拉菜单
        const $currentDropdown = $(this).closest('.dropdown');
        const $currentMenu = $currentDropdown.find('.dropdown-menu');
        // 关闭其他下拉菜单
        $dropdown.not($currentDropdown).removeClass("open");
        $dropdownMenu.not($currentMenu).addClass("hidden");
        // 切换当前下拉菜单
        $currentDropdown.toggleClass("open");
        $currentMenu.toggleClass("hidden");
    });
    // 只有点击下拉菜单按钮或菜单项外才关闭
    $(document).on('click', function(event) {
        if (!$(event.target).closest('.dropdown').length) {
            $dropdown.removeClass("open");
            $dropdownMenu.addClass("hidden");
        }
    });
});

function validateForm($form) {
    if (!$form || !$form.length) { return true; }

    var isValid = true;
    $form.find('.form-error-msg').remove();
    $form.find('.is-invalid').removeClass('is-invalid');

    $form.find('[required]').each(function(){
        var $field = $(this);
        if ($field.is(':disabled')) { return; }
        var value = $.trim($field.val());
        var invalid = false;

        if ($field.is(':checkbox') || $field.is(':radio')) {
            invalid = !$field.is(':checked');
        } else if ($field.is('select')) {
            invalid = value === '' || value === null;
        } else {
            invalid = value.length === 0;
        }

        if (invalid) {
            isValid = false;
            var $container = $field.closest('.form-control-area');
            if ($container.length === 0) {
                $container = $field.parent();
            }
            var labelText = '';
            var $label = $field.closest('.form-item').find('.form-label').first();
            if ($label.length) {
                labelText = $label.text().replace('*', '').trim();
            } else if ($field.attr('placeholder')) {
                labelText = $field.attr('placeholder');
            } else {
                labelText = '该字段';
            }

            $field.addClass('is-invalid');
            if ($container.length) {
                $('<div class="form-error-msg"><i class="fa fa-exclamation-circle"></i>' + labelText + '不能为空</div>').appendTo($container);
            }
        }
    });

    return isValid;
}

function ensureToastContainer() {
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    return container;
}

function notify(message, type, options) {
    if (typeof message === 'object') {
        options = message;
        message = options.message;
    }
    options = options || {};
    var cfg = {
        message: message || '',
        title: options.title || '',
        type: type || options.type || 'info',
        duration: options.duration || 3200,
        icon: options.icon
    };

    var container = ensureToastContainer();
    var toast = document.createElement('div');
    toast.className = 'toast toast-' + cfg.type;

    var iconHtml = cfg.icon;
    if (!iconHtml) {
        switch (cfg.type) {
            case 'success': iconHtml = '<i class="fa fa-check"></i>'; break;
            case 'danger': iconHtml = '<i class="fa fa-times"></i>'; break;
            case 'warning': iconHtml = '<i class="fa fa-exclamation"></i>'; break;
            default: iconHtml = '<i class="fa fa-info"></i>';
        }
    }

    toast.innerHTML = '' +
        '<div class="toast-icon">' + iconHtml + '</div>' +
        '<div class="toast-content">' +
            (cfg.title ? '<div class="toast-title">' + cfg.title + '</div>' : '') +
            '<div class="toast-message">' + cfg.message + '</div>' +
        '</div>' +
        '<button type="button" class="toast-close" aria-label="关闭">&times;</button>' +
        '<div class="toast-progress"><div class="toast-progress-bar"></div></div>';

    var closeBtn = toast.querySelector('.toast-close');
    closeBtn.addEventListener('click', function (e) {
        e.stopPropagation();
        removeToast(toast);
    });

    container.appendChild(toast);

    var progress = toast.querySelector('.toast-progress-bar');
    if (progress) {
        progress.style.animationDuration = cfg.duration + 'ms';
    }

    var timer = setTimeout(function () {
        removeToast(toast);
    }, cfg.duration);

    toast.addEventListener('mouseenter', function () {
        clearTimeout(timer);
        if (progress) {
            progress.style.animationPlayState = 'paused';
        }
    });

    toast.addEventListener('mouseleave', function () {
        if (progress) {
            progress.style.animationPlayState = 'running';
        }
        timer = setTimeout(function () {
            removeToast(toast);
        }, cfg.duration / 2);
    });
}

function removeToast(toast) {
    if (!toast) { return; }
    toast.style.animation = 'toast-exit 0.2s ease forwards';
    setTimeout(function () {
        if (toast && toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 180);
}

window.bootGrowl = function (message, type, duration) {
    notify({ message: message, type: type || 'info', duration: duration || 3200 });
};

/**
 * 文件上传组件初始化
 * @param {string} selector - 上传容器选择器
 * @param {object} options - 配置选项
 */
function initFileUpload(selector, options) {
    var container = document.querySelector(selector);
    if (!container) {
        console.error('文件上传容器未找到：' + selector);
        return;
    }

    var uploadArea = container.querySelector('[data-upload-area]');
    var uploadList = container.querySelector('[data-upload-list]');
    var fileInput = container.querySelector('.upload-input');
    
    if (!uploadArea || !uploadList || !fileInput) {
        console.error('文件上传组件元素不完整');
        return;
    }

    // 默认配置
    var config = {
        uploadUrl: options.uploadUrl || '/upload',
        accept: options.accept || [],
        maxFiles: options.maxFiles || 10,
        maxSize: options.maxSize || 50 * 1024 * 1024, // 默认 50MB
        autoUpload: options.autoUpload !== false, // 默认自动上传
        onSuccess: options.onSuccess || function() {},
        onError: options.onError || function() {},
        onProgress: options.onProgress || function() {}
    };

    var fileList = [];
    var uploadingCount = 0;

    // 点击上传区域触发文件选择
    uploadArea.addEventListener('click', function(e) {
        if (!uploadArea.classList.contains('disabled')) {
            fileInput.click();
        }
    });

    // 文件选择
    fileInput.addEventListener('change', function(e) {
        handleFiles(e.target.files);
        fileInput.value = ''; // 清空input，允许重复选择同一文件
    });

    // 拖拽上传
    uploadArea.addEventListener('dragover', function(e) {
        e.preventDefault();
        e.stopPropagation();
        uploadArea.classList.add('dragover');
    });

    uploadArea.addEventListener('dragleave', function(e) {
        e.preventDefault();
        e.stopPropagation();
        uploadArea.classList.remove('dragover');
    });

    uploadArea.addEventListener('drop', function(e) {
        e.preventDefault();
        e.stopPropagation();
        uploadArea.classList.remove('dragover');
        handleFiles(e.dataTransfer.files);
    });

    // 处理文件
    function handleFiles(files) {
        if (!files || files.length === 0) return;

        // 检查文件数量限制
        if (fileList.length + files.length > config.maxFiles) {
            notify({ 
                message: '最多只能上传 ' + config.maxFiles + ' 个文件', 
                type: 'warning' 
            });
            return;
        }

        Array.from(files).forEach(function(file) {
            // 验证文件
            if (!validateFile(file)) return;

            // 添加到文件列表
            var fileObj = {
                id: Date.now() + '_' + Math.random().toString(36).substr(2, 9),
                file: file,
                name: file.name,
                size: file.size,
                status: 'pending', // pending, uploading, success, error
                progress: 0,
                error: null
            };

            fileList.push(fileObj);
            renderFileItem(fileObj);

            // 自动上传
            if (config.autoUpload) {
                uploadFile(fileObj);
            }
        });
    }

    // 验证文件
    function validateFile(file) {
        // 检查文件扩展名
        if (config.accept.length > 0) {
            var ext = '.' + file.name.split('.').pop().toLowerCase();
            if (config.accept.indexOf(ext) === -1) {
                notify({ 
                    message: '不支持的文件类型：' + ext, 
                    type: 'warning' 
                });
                return false;
            }
        }

        // 检查文件大小
        if (file.size > config.maxSize) {
            notify({ 
                message: '文件 "' + file.name + '" 超过大小限制（' + formatFileSize(config.maxSize) + '）', 
                type: 'warning' 
            });
            return false;
        }

        return true;
    }

    // 渲染文件项
    function renderFileItem(fileObj) {
        var item = document.createElement('div');
        item.className = 'upload-item ' + fileObj.status;
        item.setAttribute('data-file-id', fileObj.id);
        item.innerHTML = 
            '<div class="upload-item-icon">' +
                '<i class="fa fa-file-o"></i>' +
            '</div>' +
            '<div class="upload-item-info">' +
                '<div class="upload-item-name" title="' + fileObj.name + '">' + fileObj.name + '</div>' +
                '<div class="upload-item-size">' + formatFileSize(fileObj.size) + '</div>' +
                '<div class="upload-item-progress" style="display:none;">' +
                    '<div class="upload-item-progress-bar" style="width:0%"></div>' +
                '</div>' +
            '</div>' +
            '<div class="upload-item-status"></div>' +
            '<button type="button" class="upload-item-remove" data-action="remove">' +
                '<i class="fa fa-times"></i>' +
            '</button>';

        // 绑定删除事件
        item.querySelector('[data-action="remove"]').addEventListener('click', function() {
            removeFile(fileObj.id);
        });

        uploadList.appendChild(item);
    }

    // 更新文件项状态
    function updateFileItem(fileObj) {
        var item = uploadList.querySelector('[data-file-id="' + fileObj.id + '"]');
        if (!item) return;

        item.className = 'upload-item ' + fileObj.status;

        var statusEl = item.querySelector('.upload-item-status');
        var progressEl = item.querySelector('.upload-item-progress');
        var progressBar = item.querySelector('.upload-item-progress-bar');

        if (fileObj.status === 'uploading') {
            statusEl.innerHTML = '<div class="upload-status-icon loading"><i class="fa fa-spinner fa-spin"></i></div>';
            progressEl.style.display = 'block';
            progressBar.style.width = fileObj.progress + '%';
        } else if (fileObj.status === 'success') {
            statusEl.innerHTML = '<div class="upload-status-icon success"><i class="fa fa-check-circle"></i></div>';
            progressEl.style.display = 'none';
        } else if (fileObj.status === 'error') {
            statusEl.innerHTML = '<div class="upload-status-icon error"><i class="fa fa-exclamation-circle"></i></div>';
            progressEl.style.display = 'none';
            item.title = fileObj.error || '上传失败';
        }
    }

    // 上传文件
    function uploadFile(fileObj) {
        if (uploadingCount >= 3) { // 限制并发上传数
            setTimeout(function() {
                uploadFile(fileObj);
            }, 1000);
            return;
        }

        fileObj.status = 'uploading';
        fileObj.progress = 0;
        updateFileItem(fileObj);
        uploadingCount++;

        var formData = new FormData();
        formData.append('files', fileObj.file);

        var xhr = new XMLHttpRequest();

        // 上传进度
        xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
                fileObj.progress = Math.round((e.loaded / e.total) * 100);
                updateFileItem(fileObj);
                config.onProgress(fileObj, e);
            }
        });

        // 上传完成
        xhr.addEventListener('load', function() {
            uploadingCount--;
            try {
                var response = JSON.parse(xhr.responseText);
                if (xhr.status === 200 && response.success) {
                    fileObj.status = 'success';
                    updateFileItem(fileObj);
                    config.onSuccess(fileObj, response);
                } else {
                    fileObj.status = 'error';
                    fileObj.error = response.resultValue || response.message || '上传失败';
                    updateFileItem(fileObj);
                    config.onError(fileObj, fileObj.error);
                }
            } catch (e) {
                fileObj.status = 'error';
                fileObj.error = '服务器响应格式错误';
                updateFileItem(fileObj);
                config.onError(fileObj, fileObj.error);
            }
        });

        // 上传错误
        xhr.addEventListener('error', function() {
            uploadingCount--;
            fileObj.status = 'error';
            fileObj.error = '网络错误';
            updateFileItem(fileObj);
            config.onError(fileObj, fileObj.error);
        });

        xhr.open('POST', config.uploadUrl, true);
        xhr.send(formData);
    }

    // 删除文件
    function removeFile(fileId) {
        var index = fileList.findIndex(function(f) { return f.id === fileId; });
        if (index === -1) return;

        var fileObj = fileList[index];
        
        // 如果正在上传，可以取消上传（需要保存xhr对象）
        if (fileObj.status === 'uploading') {
            notify({ message: '文件正在上传中，无法删除', type: 'warning' });
            return;
        }

        // 移除DOM
        var item = uploadList.querySelector('[data-file-id="' + fileId + '"]');
        if (item) {
            item.style.animation = 'fadeOut 0.3s ease-out';
            setTimeout(function() {
                item.parentNode.removeChild(item);
            }, 300);
        }

        // 移除数据
        fileList.splice(index, 1);
    }

    // 格式化文件大小
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 B';
        var k = 1024;
        var sizes = ['B', 'KB', 'MB', 'GB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
    }

    // 公开方法
    return {
        getFiles: function() { return fileList; },
        clearFiles: function() {
            fileList = [];
            uploadList.innerHTML = '';
        },
        uploadAll: function() {
            fileList.filter(function(f) { 
                return f.status === 'pending'; 
            }).forEach(function(f) {
                uploadFile(f);
            });
        }
    };
}

/**
 * 初始化多选下拉框组件
 * @param {string} wrapperId - 包装器元素的ID
 * @param {object} options - 配置选项
 *   - linkTo: 联动的目标下拉框ID
 *   - onSelectChange: 选择变化时的回调函数
 */
function initMultiSelect(wrapperId, options) {
    options = options || {};
    var $wrapper = $('#' + wrapperId);
    if (!$wrapper.length) {
        console.warn('[initMultiSelect] 包装器元素未找到:', wrapperId);
        return;
    }
    
    // 防止重复初始化
    if ($wrapper.data('multiSelectInitialized')) {
        console.log('[initMultiSelect] 已经初始化过，跳过:', wrapperId);
        return;
    }
    $wrapper.data('multiSelectInitialized', true);
    
    var $trigger = $wrapper.find('.dbsyncer-multi-select-trigger');
    var $dropdown = $wrapper.find('.dbsyncer-multi-select-dropdown');
    var $search = $wrapper.find('.dbsyncer-multi-select-search');
    var $options = $wrapper.find('.dbsyncer-multi-select-option');
    var $checkboxes = $options.find('input[type="checkbox"]');
    var $select = $wrapper.find('select');
    var $actions = $wrapper.find('.dbsyncer-multi-select-action-btn');
    
    // 调试信息
    console.log('[initMultiSelect] 初始化:', {
        wrapperId: wrapperId,
        trigger: $trigger.length,
        dropdown: $dropdown.length,
        options: $options.length,
        checkboxes: $checkboxes.length,
        select: $select.length,
        actions: $actions.length
    });
    
    // 检查必要元素
    if (!$trigger.length || !$dropdown.length) {
        console.error('[initMultiSelect] 缺少必要元素:', wrapperId);
        return;
    }
    
    // 切换下拉框显示/隐藏
    $trigger.off('click').on('click', function(e) {
        // 如果点击的是搜索框，不切换下拉框
        if ($(e.target).hasClass('dbsyncer-multi-select-search')) {
            return;
        }
        
        var isOpen = $wrapper.hasClass('open');
        // 关闭所有其他打开的下拉框
        $('.dbsyncer-multi-select').removeClass('open');
        
        if (!isOpen) {
            $wrapper.addClass('open');
            $search.focus();
        }
    });
    
    // 点击外部关闭下拉框
    $(document).off('click.multiSelect_' + wrapperId).on('click.multiSelect_' + wrapperId, function(e) {
        if (!$wrapper.is(e.target) && $wrapper.has(e.target).length === 0) {
            $wrapper.removeClass('open');
        }
    });
    
    // 搜索功能
    $search.off('input').on('input', function() {
        var searchText = $(this).val().toLowerCase();
        $options.each(function() {
            var text = $(this).find('span').text().toLowerCase();
            if (text.indexOf(searchText) > -1) {
                $(this).removeClass('hidden');
            } else {
                $(this).addClass('hidden');
            }
        });
    });
    
    // 复选框变化事件
    $checkboxes.off('change').on('change', function() {
        updateSelectValue();
        if (options.onSelectChange) {
            options.onSelectChange();
        }
        
        // 如果配置了联动，自动匹配相似项
        if (options.linkTo) {
            autoMatchSimilarItems(wrapperId, options.linkTo);
        }
    });
    
    // 更新隐藏的select元素值
    function updateSelectValue() {
        var selectedValues = [];
        $checkboxes.filter(':checked').each(function() {
            selectedValues.push($(this).val());
        });
        $select.val(selectedValues);
    }
    
    // 操作按钮事件
    $actions.eq(0).off('click').on('click', function(e) {
        e.stopPropagation();
        // 全选（仅可见项）
        $options.filter(':not(.hidden)').find('input[type="checkbox"]').prop('checked', true);
        updateSelectValue();
        if (options.onSelectChange) {
            options.onSelectChange();
        }
    });
    
    $actions.eq(1).off('click').on('click', function(e) {
        e.stopPropagation();
        // 取消全选
        $checkboxes.prop('checked', false);
        updateSelectValue();
        if (options.onSelectChange) {
            options.onSelectChange();
        }
    });
    
    $actions.eq(2).off('click').on('click', function(e) {
        e.stopPropagation();
        // 取消过滤
        $search.val('');
        $options.removeClass('hidden');
    });
    
    $actions.eq(3).off('click').on('click', function(e) {
        e.stopPropagation();
        // 过滤：只显示已选中的项
        $options.each(function() {
            var $checkbox = $(this).find('input[type="checkbox"]');
            if ($checkbox.prop('checked')) {
                $(this).removeClass('hidden');
            } else {
                $(this).addClass('hidden');
            }
        });
    });
    
    // 阻止选项点击时关闭下拉框
    $options.off('click').on('click', function(e) {
        e.stopPropagation();
        var $checkbox = $(this).find('input[type="checkbox"]');
        // 如果点击的不是复选框本身，则切换复选框状态
        if (!$(e.target).is('input[type="checkbox"]')) {
            $checkbox.prop('checked', !$checkbox.prop('checked')).trigger('change');
        }
    });
    
    // 初始化select值
    updateSelectValue();
}

/**
 * 增强 select 下拉框功能（替代 Bootstrap selectpicker）
 * @param {jQuery} $select - jQuery 选择的 select 元素
 * @returns {jQuery} 返回增强后的 select 元素
 */
function enhanceSelect($select) {
    if (!$select || !$select.length) {
        return $select;
    }
    
    // 标记已经增强过，避免重复处理
    if ($select.data('enhanced')) {
        return $select;
    }
    $select.data('enhanced', true);
    
    // 添加change事件的自定义触发器
    // 当select的值改变时，触发自定义事件
    $select.on('change', function() {
        // 触发自定义事件，兼容之前的 Bootstrap Select 事件
        $(this).trigger('select:changed');
    });
    
    return $select;
}

/**
 * 批量增强页面中所有的 select 元素
 */
function enhanceAllSelects() {
    $('select').each(function() {
        enhanceSelect($(this));
    });
}

/**
 * 自动匹配相似表名
 * @param {string} sourceWrapperId - 源下拉框ID
 * @param {string} targetWrapperId - 目标下拉框ID
 */
function autoMatchSimilarItems(sourceWrapperId, targetWrapperId) {
    var $sourceWrapper = $('#' + sourceWrapperId);
    var $targetWrapper = $('#' + targetWrapperId);
    
    if (!$sourceWrapper.length || !$targetWrapper.length) return;
    
    var $sourceCheckboxes = $sourceWrapper.find('.dbsyncer-multi-select-option input[type="checkbox"]:checked');
    var $targetOptions = $targetWrapper.find('.dbsyncer-multi-select-option');
    
    // 取消目标所有选中
    $targetWrapper.find('input[type="checkbox"]').prop('checked', false);
    
    // 遍历源选中项，匹配目标项
    $sourceCheckboxes.each(function() {
        var sourceValue = $(this).val();
        // 优先精确匹配
        var $exactMatch = $targetOptions.find('input[type="checkbox"][value="' + sourceValue + '"]');
        if ($exactMatch.length) {
            $exactMatch.prop('checked', true);
        } else {
            // 模糊匹配：找到包含相同单词的项
            $targetOptions.each(function() {
                var $targetCheckbox = $(this).find('input[type="checkbox"]');
                var targetValue = $targetCheckbox.val();
                if (targetValue && targetValue.toLowerCase().indexOf(sourceValue.toLowerCase()) > -1) {
                    $targetCheckbox.prop('checked', true);
                    return false; // 找到一个就退出
                }
            });
        }
    });
    
    // 更新目标select值
    var selectedValues = [];
    $targetWrapper.find('input[type="checkbox"]:checked').each(function() {
        selectedValues.push($(this).val());
    });
    $targetWrapper.find('select').val(selectedValues);
}