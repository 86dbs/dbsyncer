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
(function(window, $) {
    'use strict';
    
    // 全局变量：跟踪当前打开的 dbSelect 实例
    window.dbSelectCurrentOpenInstance = null;
    
    // HTML转义
    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
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
    
    $.fn.dbSelect = function(options) {
        options = options || {};
        
        const $select = $(this);
        if (!$select.length) return null;
        
        // 防止重复初始化，直接返回已有的 API 实例
        if ($select.data('dbSelectInitialized')) {
            return $select.data('dbSelect');
        }
        $select.data('dbSelectInitialized', true);

        // 默认配置
        let defaultValue = options.defaultValue || null;

        // 如果没有设置 defaultValue，尝试从 HTML 中读取 option:selected 的值
        if (!defaultValue && $select.is('select')) {
            const $selectedOption = $select.find('option:selected');
            if ($selectedOption.length > 0) {
                const selectedValue = $selectedOption.val();
                if (selectedValue && selectedValue !== '') {
                    defaultValue = selectedValue;
                }
            }
        }

        const config = {
            type: options.type || 'single',
            data: options.data || [],
            defaultValue: defaultValue,
            disabled: options.disabled || false,
            customButtons: (options.customButtons || []).slice(0, 2), // 最多2个
            onSelect: options.onSelect || function() {},
            onReady: options.onReady || function() {},
            onCustomButton: options.onCustomButton || function() {}
        };

        // 从 HTML 中读取选项
        if ($select.is('select') && config.data.length === 0) {
            config.data = [];
            $select.find('option').each(function() {
                const $option = $(this);
                const item = {
                    label: $option.text(),
                    value: $option.val(),
                    disabled: $option.prop('disabled')
                };
                
                // 读取所有 data-* 属性
                const dataAttrs = {};
                $.each($option[0].attributes, function(i, attr) {
                    if (attr.name.startsWith('data-')) {
                        // 将 data-type 转换为 type，data-custom-attr 转换为 customAttr
                        const key = attr.name.replace(/^data-/, '').replace(/-([a-z])/g, function(g) {
                            return g[1].toUpperCase();
                        });
                        dataAttrs[key] = attr.value;
                    }
                });
                // 如果有 data 属性，则存储
                if (Object.keys(dataAttrs).length > 0) {
                    item.data = dataAttrs;
                }
                
                config.data.push(item);
            });
        }

        // 创建 Select 组件 HTML（使用更唯一的 ID，避免快速重复初始化时冲突）
        const timestamp = Date.now();
        const random = Math.random().toString(36).substring(2, 9);
        const selectId = 'dbsyncer-select-' + timestamp + '-' + random;
        const dropdownId = 'dbsyncer-select-dropdown-' + timestamp + '-' + random;
        
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

                // 单选时隐藏复选框，多选时显示
                const checkboxClass = config.type === 'single' ? 'dbsyncer-select-option-checkbox hidden' : 'dbsyncer-select-option-checkbox';

                const $option = $(`
                    <label class="dbsyncer-select-option ${matches ? '' : 'hidden'} ${isSelected ? 'selected' : ''} ${item.disabled ? 'disabled' : ''}">
                        <input type="${inputType}" value="${escapeAttr(item.value)}" ${isSelected ? 'checked' : ''} ${item.disabled ? 'disabled' : ''} data-dbselect-internal>
                        <div class="${checkboxClass}"></div>
                        <span class="dbsyncer-select-option-text">${escapeHtml(item.label)}</span>
                    </label>
                `);

                $option.on('click.dbSelect-' + selectId, function(e) {
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
                $options.find('input[type="radio"]').prop('checked', false);
                $option.addClass('selected');
                $option.find('input[type="radio"]').prop('checked', true);
                // 单选时自动关闭下拉菜单
                closeDropdown();
            } else {
                const index = selectedValues.indexOf(value);
                const $checkbox = $option.find('input[type="checkbox"]');
                if (index > -1) {
                    selectedValues.splice(index, 1);
                    $option.removeClass('selected');
                    $checkbox.prop('checked', false);
                } else {
                    selectedValues.push(value);
                    $option.addClass('selected');
                    $checkbox.prop('checked', true);
                }
            }

            updateDisplay();
            triggerSelectEvent();
        }

        // 同步原始 select 的 options
        function syncSelectOptions() {
            if (!$select.is('select')) return;
            
            // 获取当前 options（保留初始的 HTML options，避免重复添加）
            const existingOptions = {};
            $select.find('option').each(function() {
                existingOptions[$(this).val()] = true;
            });
            
            // 添加缺失的 options
            config.data.forEach(function(item) {
                if (!existingOptions[item.value]) {
                    const $option = $('<option></option>')
                        .attr('value', item.value)
                        .text(item.label);
                    if (item.disabled) {
                        $option.prop('disabled', true);
                    }
                    $select.append($option);
                }
            });
        }

        // 更新显示
        function updateDisplay() {
            // 更新原生 select
            if ($select.is('select')) {
                // 确保原始 select 的 options 与 config.data 同步
                syncSelectOptions();
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
                            $tag.find('.dbsyncer-select-tag-remove').on('click.dbSelect-' + selectId, function(e) {
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
                            $tag.find('.dbsyncer-select-tag-remove').on('click.dbSelect-' + selectId, function(e) {
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
                // 先关闭其他已打开的 dbSelect 实例
                if (window.dbSelectCurrentOpenInstance && window.dbSelectCurrentOpenInstance !== api) {
                    window.dbSelectCurrentOpenInstance.close();
                }
                
                $component.addClass('open');
                $dropdown.removeClass('hidden');
                
                // 智能定位：检测是否需要向上展开
                adjustDropdownPosition();
                
                $searchInput.focus();
                renderOptions('');
                
                // 更新全局当前打开的实例
                window.dbSelectCurrentOpenInstance = api;
            }
        }

        // 调整下拉菜单位置（智能向上/向下展开）
        function adjustDropdownPosition() {
            const $trigger = $component.find('.dbsyncer-select-trigger');
            const triggerRect = $trigger[0].getBoundingClientRect();
            const dropdownHeight = $dropdown.outerHeight() || 300; // 默认最大高度
            const viewportHeight = window.innerHeight;
            const spaceBelow = viewportHeight - triggerRect.bottom;
            const spaceAbove = triggerRect.top;
            
            // 移除之前的定位类
            $dropdown.removeClass('dbsyncer-select-dropdown-up');
            
            // 如果下方空间不足，且上方空间足够，则向上展开
            if (spaceBelow < dropdownHeight && spaceAbove > spaceBelow) {
                $dropdown.addClass('dbsyncer-select-dropdown-up');
            }
        }

        // 关闭下拉菜单
        function closeDropdown() {
            $component.removeClass('open');
            $dropdown.addClass('hidden');
            $searchInput.val('');
            $searchClear.removeClass('active');
            renderOptions('');
            
            // 如果当前实例就是打开的实例，清空全局变量
            if (window.dbSelectCurrentOpenInstance === api) {
                window.dbSelectCurrentOpenInstance = null;
            }
        }

        // 监听窗口滚动和大小改变，重新调整下拉菜单位置
        function handleWindowEvents() {
            if (!$dropdown.hasClass('hidden')) {
                adjustDropdownPosition();
            }
        }

        // 绑定窗口事件（使用命名空间以便后续移除）
        $(window).on('scroll.dbSelect-' + selectId, handleWindowEvents);
        $(window).on('resize.dbSelect-' + selectId, handleWindowEvents);
        
        // 组件销毁时移除事件监听
        $select.on('remove', function() {
            $(window).off('scroll.dbSelect-' + selectId);
            $(window).off('resize.dbSelect-' + selectId);
        });

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

        // 绑定事件
        $trigger.on('click.dbSelect-' + selectId, function(e) {
            e.stopPropagation();
            if ($dropdown.hasClass('hidden')) {
                openDropdown();
            } else {
                closeDropdown();
            }
        });

        // 搜索
        $searchInput.on('input.dbSelect-' + selectId, function() {
            const text = $(this).val();
            $searchClear.toggleClass('active', text.length > 0);
            renderOptions(text);
        });

        // 清除搜索
        $searchClear.on('click.dbSelect-' + selectId, function(e) {
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

            $selectAllBtn.on('click.dbSelect-' + selectId, function(e) {
                e.preventDefault();
                e.stopPropagation();
                selectAll();
            });

            $deselectAllBtn.on('click.dbSelect-' + selectId, function(e) {
                e.preventDefault();
                e.stopPropagation();
                deselectAll();
            });

            $actions.append($selectAllBtn).append($deselectAllBtn);
        }

        // 添加自定义按钮
        config.customButtons.forEach(function(btn, index) {
            const $btn = $(`<button class="dbsyncer-select-action-btn">${escapeHtml(btn.text)}</button>`);
            $btn.on('click.dbSelect-' + selectId, function(e) {
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

        // 初始化显示
        renderOptions('');
        updateDisplay();

        // 如果没有设置默认值，检查是否有 selected 属性或默认选中第一个
        if (selectedValues.length === 0 && $select.is('select')) {
            // 检查是否有 selected 属性的选项
            const $selectedOptions = $select.find('option:selected');
            if ($selectedOptions.length > 0) {
                selectedValues = [];
                $selectedOptions.each(function() {
                    const val = $(this).val();
                    if (val && val !== '') {
                        selectedValues.push(val);
                    }
                });
            } else {
                // 没有 selected 属性，单选模式下默认选中第一个非空选项
                if (config.type === 'single') {
                    const $firstOption = $select.find('option').filter(function() {
                        return $(this).val() && $(this).val() !== '';
                    }).first();
                    if ($firstOption.length) {
                        selectedValues = [$firstOption.val()];
                    }
                }
            }
            // 重新更新显示
            if (selectedValues.length > 0) {
                updateDisplay();
                renderOptions('');
                triggerSelectEvent();
            }
        }

        // 创建 API 对象
        const api = {
            $element: $select,
            $component: $component,
            getValues: function() { return selectedValues; },
            setValues: function(values) {
                const inputValues = Array.isArray(values) ? values : [values];
                // 过滤掉无效值（只保留在 config.data 中存在的值）
                selectedValues = inputValues.filter(function(value) {
                    return config.data.some(function(item) {
                        return item.value === value;
                    });
                });
                updateDisplay();
                // 无论下拉菜单是否打开，都更新选项状态
                renderOptions($searchInput.val());
                triggerSelectEvent();
                return this;
            },
            setData: function(newData) {
                // 更新选项数据
                config.data = newData || [];
                
                // 如果是原始 select 元素，清空并重新填充 options
                if ($select.is('select')) {
                    $select.empty();
                    config.data.forEach(function(item) {
                        const $option = $('<option></option>')
                            .attr('value', item.value)
                            .text(item.label);
                        if (item.disabled) {
                            $option.prop('disabled', true);
                        }
                        $select.append($option);
                    });
                }
                
                // 清空搜索框
                $searchInput.val('');
                // 清空已选值（因为数据已经变了）
                selectedValues = [];
                // 重新初始化默认值
                if (config.defaultValue) {
                    selectedValues = Array.isArray(config.defaultValue) ? config.defaultValue : [config.defaultValue];
                } else {
                    // 如果没有默认值，自动选中第一个非空选项
                    if (config.data.length > 0) {
                        const firstItem = config.data.find(d => d.value && d.value !== '');
                        if (firstItem) {
                            selectedValues = [firstItem.value];
                        }
                    }
                }
                // 重新渲染
                updateDisplay();
                renderOptions('');
                triggerSelectEvent();
                return this;
            },
            getData: function() {
                return config.data;
            },
            // 获取选中项的完整信息（包括 data 属性）
            getSelectedItems: function() {
                return selectedValues.map(function(value) {
                    const item = config.data.find(d => d.value === value);
                    if (item) {
                        return {
                            value: item.value,
                            label: item.label,
                            data: item.data || {}
                        };
                    }
                    return { value: value, label: value, data: {} };
                });
            },
            // 获取指定选中项的指定属性值
            getAttribute: function(attributeName, value) {
                // 如果指定了 value，获取该值的属性；否则获取第一个选中项的属性
                const targetValue = value !== undefined ? value : (selectedValues[0] || null);
                if (!targetValue) return null;
                
                const item = config.data.find(d => d.value === targetValue);
                if (item && item.data) {
                    return item.data[attributeName] || null;
                }
                return null;
            },
            clear: function() {
                selectedValues = [];
                updateDisplay();
                renderOptions('');
                triggerSelectEvent();
                return this;
            },
            close: function() {
                closeDropdown();
                return this;
            },
            destroy: function() {
                // 先关闭下拉菜单
                closeDropdown();
                
                // 移除文档级别的事件监听
                $(document).off('click.dbselect_' + selectId);
                
                // 移除窗口事件监听
                $(window).off('scroll.dbSelect-' + selectId);
                $(window).off('resize.dbSelect-' + selectId);
                
                // 移除组件内所有使用命名空间的事件监听（包括所有子元素）
                if ($component && $component.length) {
                    // 清理组件及其所有子元素上的事件
                    // 使用命名空间清理，确保只清理当前实例的事件
                    const namespace = '.dbSelect-' + selectId;
                    $component.off(namespace);
                    $component.find('*').off(namespace);
                }
                
                // 移除组件 DOM（这会自动清理所有绑定的事件）
                if ($component && $component.length) {
                    $component.remove();
                }
                
                // 显示原始 select 元素
                if ($select && $select.length) {
                    $select.show();
                }
                
                // 清理数据
                $select.removeData('dbSelectInitialized');
                $select.removeData('dbSelect');
                // 如果销毁的是当前打开的实例，清空全局变量
                if (window.dbSelectCurrentOpenInstance === this) {
                    window.dbSelectCurrentOpenInstance = null;
                }
                return this;
            }
        };

        // 保存 API 到组件和原始元素
        $component.data('dbSelect', api);
        $select.data('dbSelect', api);
        // 触发初始化完成事件
        config.onReady(selectedValues);

        return api;
    };
    
})(window, jQuery);