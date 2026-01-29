/**
 * Radio Group 美化的圆形单选组件
 * 保持传统圆形 radio 外观，但更精致优雅
 * 使用方式：$('.radio-group').radioGroup(options)
 * 
 * @param {Object} options - 配置选项
 *   - theme: 主题色 ('primary', 'success', 'warning', 'danger', 'info')，默认 'primary'
 *   - size: 尺寸 ('sm', 'md', 'lg')，默认 'md'
 *   - layout: 布局方式 ('horizontal', 'vertical', 'inline')，默认 'horizontal'
 *   - bordered: 是否显示边框样式，默认 false
 *   - onChange: 选中项变化时的回调函数，参数为 (value, label)
 * 
 * @example
 * // 基本用法
 * const radioGroup = $('#myRadioGroup').radioGroup({
 *     theme: 'success',
 *     size: 'lg',
 *     bordered: true,
 *     onChange: function(value, label) {
 *         console.log('选中:', value, label);
 *     }
 * });
 * // 获取当前值
 * const value = radioGroup.getValue();
 * // 设置值
 * radioGroup.setValue('full');
 * 
 * @example
 * // HTML 结构
 * <div class="radio-group" id="myRadioGroup">
 *     <input type="radio" name="mode" value="full" checked />
 *     <label>全量</label>
 *     <input type="radio" name="mode" value="increment" />
 *     <label>增量</label>
 *     <input type="radio" name="mode" value="fullToIncrement" disabled />
 *     <label>全量+增量</label>
 * </div>
 */
(function($) {
    'use strict';
    
    // HTML转义函数（如果全局没有定义，则使用本地定义）
    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
    $.fn.radioGroup = function(options) {
        options = options || {};
        
        // 默认配置
        const config = $.extend({
            theme: 'primary',
            size: 'md',
            layout: 'horizontal',
            bordered: false,
            onChange: null
        }, options);
        
        // 如果只有一个元素，直接返回 API 实例
        if (this.length === 1) {
            const $container = $(this[0]);
            
            // 防止重复初始化，直接返回已有的 API 实例
            if ($container.data('radioGroupInitialized')) {
                return $container.data('radioGroup');
            }
            $container.data('radioGroupInitialized', true);
            
            return initRadioGroup($container, config);
        }
        
        // 多个元素，返回 jQuery 对象（向后兼容）
        return this.each(function() {
            const $container = $(this);
            
            // 防止重复初始化
            if ($container.data('radioGroupInitialized')) {
                return;
            }
            $container.data('radioGroupInitialized', true);
            
            initRadioGroup($container, config);
        });
    };
    
    // 初始化 radioGroup 的内部函数
    function initRadioGroup($container, config) {
        // 添加主题和尺寸类
        if (config.theme && config.theme !== 'primary') {
            $container.addClass('radio-group-' + config.theme);
        }
        if (config.size && config.size !== 'md') {
            $container.addClass('radio-group-' + config.size);
        }
        
        // 添加布局类
        if (config.layout === 'vertical') {
            $container.addClass('radio-group-vertical');
        } else if (config.layout === 'inline') {
            $container.addClass('radio-group-inline');
        }
        
        // 添加边框样式
        if (config.bordered) {
            $container.addClass('radio-group-bordered');
        }
        
        // 获取所有 radio 和对应的 label
        const $radios = $container.find('input[type="radio"]');
        if ($radios.length === 0) {
            return null;
        }
        
        // 包装每个 radio 和 label
        $radios.each(function() {
            const $radio = $(this);
            const $label = $radio.next('label');
            
            // 创建包装元素
            const $item = $('<div class="radio-group-item"></div>');
            
            // 添加禁用状态
            if ($radio.prop('disabled')) {
                $item.addClass('disabled');
            }
            
            // 添加选中状态
            const isChecked = $radio.prop('checked');
            if (isChecked) {
                $item.addClass('active');
            }
            
            // 创建标签内容
            const labelText = $label.length ? $label.text() : $radio.val();
            const $itemLabel = $('<span class="radio-group-item-label">' + escapeHtml(labelText) + '</span>');
            
            // 将 radio 和标签移入包装元素
            $item.append($radio);
            $item.append($itemLabel);
            
            // 移除原始 label（已经提取文本）
            if ($label.length) {
                $label.remove();
            }
            
            // 将包装元素插入容器
            $container.append($item);
        });
        
        // 确保初始选中状态正确应用
        setTimeout(function() {
            const $checked = $container.find('input[type="radio"]:checked');
            if ($checked.length) {
                $container.find('.radio-group-item').removeClass('active');
                $checked.closest('.radio-group-item').addClass('active');
            }
        }, 0);
        
        // 事件委托：点击项目时选中对应的 radio
        $container.on('click', '.radio-group-item:not(.disabled)', function() {
            const $item = $(this);
            const $radio = $item.find('input[type="radio"]');
            // 取消所有选中状态
            $container.find('.radio-group-item').removeClass('active');
            // 设置当前项为选中
            $item.addClass('active');
            $radio.prop('checked', true).trigger('change');
        });
        
        // 监听 radio 变化事件（支持外部程序直接操作 radio）
        $container.on('change', 'input[type="radio"]', function() {
            const $radio = $(this);
            const $item = $radio.closest('.radio-group-item');
            
            // 更新所有项的选中状态
            $container.find('.radio-group-item').removeClass('active');
            $item.addClass('active');
            
            // 触发回调
            if (config.onChange && typeof config.onChange === 'function') {
                const value = $radio.val();
                const label = $item.find('.radio-group-item-label').text();
                config.onChange(value, label);
            }
        });
        
        // 键盘导航支持
        $container.on('keydown', 'input[type="radio"]', function(e) {
            const $current = $(this).closest('.radio-group-item');
            let $target = null;
            
            if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
                // 下一项
                $target = $current.nextAll('.radio-group-item:not(.disabled)').first();
                if (!$target.length) {
                    $target = $container.find('.radio-group-item:not(.disabled)').first();
                }
                e.preventDefault();
            } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
                // 上一项
                $target = $current.prevAll('.radio-group-item:not(.disabled)').first();
                if (!$target.length) {
                    $target = $container.find('.radio-group-item:not(.disabled)').last();
                }
                e.preventDefault();
            }
            
            if ($target && $target.length) {
                $target.find('input[type="radio"]').focus().click();
            }
        });
        
        // API 方法
        const api = {
            getValue: function() {
                return $container.find('input[type="radio"]:checked').val();
            },
            setValue: function(value) {
                const $radio = $container.find('input[type="radio"][value="' + value + '"]');
                if ($radio.length && !$radio.prop('disabled')) {
                    $radio.trigger('click');
                }
                return this;
            },
            disable: function(value) {
                if (value === undefined) {
                    // 禁用所有
                    $container.find('.radio-group-item').addClass('disabled');
                    $container.find('input[type="radio"]').prop('disabled', true);
                } else {
                    // 禁用指定值
                    const $radio = $container.find('input[type="radio"][value="' + value + '"]');
                    $radio.prop('disabled', true);
                    $radio.closest('.radio-group-item').addClass('disabled');
                }
                return this;
            },
            enable: function(value) {
                if (value === undefined) {
                    // 启用所有
                    $container.find('.radio-group-item').removeClass('disabled');
                    $container.find('input[type="radio"]').prop('disabled', false);
                } else {
                    // 启用指定值
                    const $radio = $container.find('input[type="radio"][value="' + value + '"]');
                    $radio.prop('disabled', false);
                    $radio.closest('.radio-group-item').removeClass('disabled');
                }
                return this;
            },
            destroy: function() {
                $container.off('click change keydown');
                $container.find('.radio-group-item').each(function() {
                    const $item = $(this);
                    const $radio = $item.find('input[type="radio"]');
                    const $label = $('<label>' + $item.find('.radio-group-item-label').text() + '</label>');
                    $radio.insertBefore($item);
                    $label.insertAfter($radio);
                    $item.remove();
                });
                $container.removeData('radioGroupInitialized');
                $container.removeClass(function(index, className) {
                    return (className.match(/\bradio-group-\S+/g) || []).join(' ');
                });
                return this;
            }
        };
        
        // 保存 API 到 data
        $container.data('radioGroup', api);
        
        return api;
    }
    
})(jQuery);
