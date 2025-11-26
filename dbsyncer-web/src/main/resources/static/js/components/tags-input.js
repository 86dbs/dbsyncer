/**
 * 初始化标签输入框（Tagsinput）
 * 将带有 data-role="tagsinput" 的输入框转换为标签输入框
 * 支持添加/删除标签，提交时用逗号拼接
 */
(function(window) {
    'use strict';
    
    // HTML 转义
    function escapeHtml(text) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(text);
        }
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    
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
    
    // 导出到全局
    window.initMultipleInputTags = initMultipleInputTags;
    
})(window);

