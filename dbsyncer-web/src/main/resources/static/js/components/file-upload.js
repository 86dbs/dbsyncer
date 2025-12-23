/**
 * 文件上传组件初始化
 * @param {string} selector - 上传容器选择器
 * @param {object} options - 配置选项
 */
(function(window) {
    'use strict';
    
    // 依赖检查
    if (typeof window.notify !== 'function') {
        console.error('[initFileUpload] 依赖 notify 函数未找到');
        return;
    }
    
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
                window.notify({ 
                    message: '最多只能上传 ' + config.maxFiles + ' 个文件', 
                    type: 'warning' 
                });
                return;
            }

            Array.from(files).forEach(function(file) {
                // 验证文件
                if (!validateFile(file)) return;

                // 添加到文件列表
                const fileObj = {
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
                const ext = '.' + file.name.split('.').pop().toLowerCase();
                if (config.accept.indexOf(ext) === -1) {
                    window.notify({ 
                        message: '不支持的文件类型：' + ext, 
                        type: 'warning' 
                    });
                    return false;
                }
            }

            // 检查文件大小
            if (file.size > config.maxSize) {
                window.notify({ 
                    message: '文件 "' + file.name + '" 超过大小限制（' + formatFileSize(config.maxSize) + '）', 
                    type: 'warning' 
                });
                return false;
            }

            return true;
        }

        // 渲染文件项
        function renderFileItem(fileObj) {
            const item = document.createElement('div');
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
            const item = uploadList.querySelector('[data-file-id="' + fileObj.id + '"]');
            if (!item) return;

            item.className = 'upload-item ' + fileObj.status;

            const statusEl = item.querySelector('.upload-item-status');
            const progressEl = item.querySelector('.upload-item-progress');
            const progressBar = item.querySelector('.upload-item-progress-bar');

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

            const formData = new FormData();
            formData.append('files', fileObj.file);

            const xhr = new XMLHttpRequest();

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
                    const response = JSON.parse(xhr.responseText);
                    if (xhr.status === 200 && response.success) {
                        fileObj.status = 'success';
                        updateFileItem(fileObj);
                        config.onSuccess(fileObj, response);
                    } else {
                        fileObj.status = 'error';
                        fileObj.error = response.message || '上传失败';
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
                window.notify({ message: '文件正在上传中，无法删除', type: 'warning' });
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
    
    // 导出到全局
    window.initFileUpload = initFileUpload;
    
})(window);

