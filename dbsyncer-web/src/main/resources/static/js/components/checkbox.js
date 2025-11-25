/**
 * Checkbox Group 美化的方形复选框组件
 * 保持传统方形 checkbox 外观，但更精致优雅
 * 
 * @param {Object} options - 配置选项
 *   - theme: 主题色 ('primary', 'success', 'warning', 'danger', 'info')，默认 'primary'
 *   - size: 尺寸 ('sm', 'md', 'lg')，默认 'md'
 *   - layout: 布局方式 ('horizontal', 'vertical', 'inline')，默认 'horizontal'
 *   - bordered: 是否显示边框样式，默认 false
 *   - showSelectAll: 是否显示全选/取消全选按钮（复选框组），默认 false
 *   - onChange: 选中项变化时的回调函数，参数为 (values, labels)
 *
 * 使用方式：
 *   - $('input[type="checkbox"]').checkboxGroup(options)  // 直接选择 input
 * @example
 * $('.tableGroupCheckbox').checkboxGroup({
 *     theme: 'info',
 *     onChange: function(values) {
 *         console.log('选中:', values);
 *     }
 * });
 */
(function($) {
    'use strict';
    
    // HTML转义函数
    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    // 创建单个 checkbox 的包装
    function wrapCheckbox($checkbox, config) {
        const $label = $checkbox.next('label');
        
        // 创建包装元素
        const $item = $('<div class="checkbox-group-item"></div>');
        
        // 添加禁用状态
        if ($checkbox.prop('disabled')) {
            $item.addClass('disabled');
        }
        
        // 添加选中状态
        const isChecked = $checkbox.prop('checked');
        if (isChecked) {
            $item.addClass('active');
        }
        
        // 创建标签内容
        let labelText = '';
        if ($label.length) {
            labelText = $label.text();
        } else if ($checkbox.attr('id')) {
            const $labelFor = $('label[for="' + $checkbox.attr('id') + '"]');
            if ($labelFor.length) {
                labelText = $labelFor.text();
            } else {
                labelText = $checkbox.val() || '';
            }
        } else {
            labelText = $checkbox.val() || '';
        }
        
        const $itemLabel = $('<span class="checkbox-group-item-label">' + escapeHtml(labelText) + '</span>');
        
        // 先在 checkbox 的位置插入 item
        $checkbox.before($item);
        
        // 然后将 checkbox 和标签移入包装元素
        $item.append($checkbox);
        $item.append($itemLabel);
        
        // 移除原始 label
        if ($label.length) {
            $label.remove();
        }
        
        return $item;
    }
    
    // 绑定单个 checkbox 的事件
    function bindCheckboxEvents($container, $item, $checkbox, config, isSingle) {
        // 点击项目时切换 checkbox 状态
        $item.on('click', function(e) {
            // 如果点击的是复选框本身，不处理，让浏览器原生事件处理
            if ($(e.target).is('input[type="checkbox"]')) {
                return;
            }
            
            if ($item.hasClass('disabled')) {
                e.preventDefault();
                return;
            }
            
            e.preventDefault();
            e.stopPropagation();
            
            // 切换选中状态
            const isChecked = $checkbox.prop('checked');
            if (isChecked) {
                $item.removeClass('active');
                $checkbox.prop('checked', false);
            } else {
                $item.addClass('active');
                $checkbox.prop('checked', true);
            }
            
            // 手动触发 change 事件
            $checkbox.trigger('change');
        });
        
        // 监听 checkbox 原生变化事件
        $checkbox.on('change', function(e) {
            // 更新视觉状态
            if ($checkbox.prop('checked')) {
                $item.addClass('active');
            } else {
                $item.removeClass('active');
            }
            
            // 触发回调
            if (config.onChange && typeof config.onChange === 'function') {
                if (isSingle) {
                    // 单个 checkbox，直接传递值
                    config.onChange($checkbox.prop('checked') ? [$checkbox.val()] : [], 
                                   $checkbox.prop('checked') ? [$item.find('.checkbox-group-item-label').text()] : []);
                } else {
                    // 多个 checkbox，需要收集所有值
                    triggerChangeEvent($container, config);
                }
            }
        });
    }
    
    // 触发变化事件（容器方式）
    function triggerChangeEvent($container, config) {
        const selectedValues = [];
        const selectedLabels = [];
        
        $container.find('input[type="checkbox"]:checked').each(function() {
            const $checkbox = $(this);
            selectedValues.push($checkbox.val());
            const $item = $checkbox.closest('.checkbox-group-item');
            selectedLabels.push($item.find('.checkbox-group-item-label').text());
        });
        
        if (config.onChange && typeof config.onChange === 'function') {
            config.onChange(selectedValues, selectedLabels);
        }
        
        $container.trigger('checkboxgroup:change', [selectedValues, selectedLabels]);
    }
    
    $.fn.checkboxGroup = function(options) {
        // 默认配置
        const config = $.extend({
            theme: 'primary',
            size: 'md',
            layout: 'horizontal',
            bordered: false,
            showSelectAll: false,
            onChange: null
        }, options);
        
        return this.each(function() {
            const $element = $(this);
            
            // 如果直接选择的是 input[type="checkbox"]，简化处理
            if ($element.is('input[type="checkbox"]')) {
                // 防止重复初始化
                if ($element.data('checkboxGroupInitialized')) {
                    return;
                }
                $element.data('checkboxGroupInitialized', true);
                
                // 创建容器（如果需要）
                let $container = $element.closest('.checkbox-group');
                if (!$container.length) {
                    $container = $('<div class="checkbox-group"></div>');
                    $element.before($container);
                    $container.append($element);
                }
                
                // 添加主题和尺寸类
                if (config.theme && config.theme !== 'primary') {
                    $container.addClass('checkbox-group-' + config.theme);
                }
                if (config.size && config.size !== 'md') {
                    $container.addClass('checkbox-group-' + config.size);
                }
                
                // 创建包装
                const $item = wrapCheckbox($element, config);
                
                // 绑定事件
                bindCheckboxEvents($container, $item, $element, config, true);
                
                // 创建简单的 API
                const api = {
                    getValue: function() {
                        return $element.prop('checked') ? $element.val() : null;
                    },
                    setValue: function(checked) {
                        $element.prop('checked', !!checked);
                        if (checked) {
                            $item.addClass('active');
                        } else {
                            $item.removeClass('active');
                        }
                        $element.trigger('change');
                        return this;
                    },
                    disable: function() {
                        $item.addClass('disabled');
                        $element.prop('disabled', true);
                        return this;
                    },
                    enable: function() {
                        $item.removeClass('disabled');
                        $element.prop('disabled', false);
                        return this;
                    }
                };
                
                $container.data('checkboxGroup', api);
                $element.data('checkboxGroup', api);
                
                return;
            }
            
            // 原有的容器方式处理
            const $container = $element;
            
            // 防止重复初始化
            if ($container.data('checkboxGroupInitialized')) {
                return;
            }
            $container.data('checkboxGroupInitialized', true);
            
            // 添加主题和尺寸类
            if (config.theme && config.theme !== 'primary') {
                $container.addClass('checkbox-group-' + config.theme);
            }
            if (config.size && config.size !== 'md') {
                $container.addClass('checkbox-group-' + config.size);
            }
            
            // 添加布局类
            if (config.layout === 'vertical') {
                $container.addClass('checkbox-group-vertical');
            } else if (config.layout === 'inline') {
                $container.addClass('checkbox-group-inline');
            }
            
            // 添加边框样式
            if (config.bordered) {
                $container.addClass('checkbox-group-bordered');
            }
            
            // 获取所有 checkbox
            const $checkboxes = $container.find('input[type="checkbox"]');
            
            if ($checkboxes.length === 0) {
                console.warn('[checkboxGroup] 未找到 checkbox 元素');
                return;
            }
            
            // 创建操作按钮区域（如果需要）
            let $actions = null;
            if (config.showSelectAll && $checkboxes.length > 1) {
                $actions = $('<div class="checkbox-group-actions"></div>');
                const $selectAllBtn = $('<button type="button" class="checkbox-group-action-btn">全选</button>');
                const $deselectAllBtn = $('<button type="button" class="checkbox-group-action-btn">取消全选</button>');
                
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
                $container.prepend($actions);
            }
            
            // 包装每个 checkbox
            $checkboxes.each(function() {
                const $checkbox = $(this);
                const $item = wrapCheckbox($checkbox, config);
                bindCheckboxEvents($container, $item, $checkbox, config, false);
            });
            
            // 全选函数
            function selectAll() {
                $container.find('.checkbox-group-item:not(.disabled)').each(function() {
                    const $item = $(this);
                    const $checkbox = $item.find('input[type="checkbox"]');
                    if (!$checkbox.prop('checked')) {
                        $item.addClass('active');
                        $checkbox.prop('checked', true).trigger('change');
                    }
                });
                triggerChangeEvent($container, config);
            }
            
            // 取消全选函数
            function deselectAll() {
                $container.find('.checkbox-group-item').each(function() {
                    const $item = $(this);
                    const $checkbox = $item.find('input[type="checkbox"]');
                    $item.removeClass('active');
                    $checkbox.prop('checked', false).trigger('change');
                });
                triggerChangeEvent($container, config);
            }
            
            // API 方法
            const api = {
                getValues: function() {
                    const values = [];
                    $container.find('input[type="checkbox"]:checked').each(function() {
                        values.push($(this).val());
                    });
                    return values;
                },
                getLabels: function() {
                    const labels = [];
                    $container.find('input[type="checkbox"]:checked').each(function() {
                        const $item = $(this).closest('.checkbox-group-item');
                        labels.push($item.find('.checkbox-group-item-label').text());
                    });
                    return labels;
                },
                setValues: function(values) {
                    values = Array.isArray(values) ? values : [values];
                    $container.find('input[type="checkbox"]').each(function() {
                        const $checkbox = $(this);
                        const value = $checkbox.val();
                        const shouldCheck = values.indexOf(value) > -1;
                        const $item = $checkbox.closest('.checkbox-group-item');
                        
                        if (shouldCheck && !$checkbox.prop('disabled')) {
                            $item.addClass('active');
                            $checkbox.prop('checked', true);
                        } else {
                            $item.removeClass('active');
                            $checkbox.prop('checked', false);
                        }
                    });
                    triggerChangeEvent($container, config);
                    return this;
                },
                selectAll: function() {
                    selectAll();
                    return this;
                },
                deselectAll: function() {
                    deselectAll();
                    return this;
                },
                disable: function(value) {
                    if (value === undefined) {
                        $container.find('.checkbox-group-item').addClass('disabled');
                        $container.find('input[type="checkbox"]').prop('disabled', true);
                    } else {
                        const $checkbox = $container.find('input[type="checkbox"][value="' + value + '"]');
                        $checkbox.prop('disabled', true);
                        $checkbox.closest('.checkbox-group-item').addClass('disabled');
                    }
                    return this;
                },
                enable: function(value) {
                    if (value === undefined) {
                        $container.find('.checkbox-group-item').removeClass('disabled');
                        $container.find('input[type="checkbox"]').prop('disabled', false);
                    } else {
                        const $checkbox = $container.find('input[type="checkbox"][value="' + value + '"]');
                        $checkbox.prop('disabled', false);
                        $checkbox.closest('.checkbox-group-item').removeClass('disabled');
                    }
                    return this;
                },
                destroy: function() {
                    $container.off('click change keydown');
                    if ($actions) {
                        $actions.remove();
                    }
                    $container.find('.checkbox-group-item').each(function() {
                        const $item = $(this);
                        const $checkbox = $item.find('input[type="checkbox"]');
                        const labelText = $item.find('.checkbox-group-item-label').text();
                        const $label = $('<label>' + labelText + '</label>');
                        
                        if ($checkbox.attr('id')) {
                            $label.attr('for', $checkbox.attr('id'));
                        }
                        
                        $checkbox.insertBefore($item);
                        $label.insertAfter($checkbox);
                        $item.remove();
                    });
                    $container.removeData('checkboxGroupInitialized');
                    $container.removeClass(function(index, className) {
                        return (className.match(/\bcheckbox-group-\S+/g) || []).join(' ');
                    });
                    return this;
                }
            };
            
            $container.data('checkboxGroup', api);
        });
    };
    
})(jQuery);
