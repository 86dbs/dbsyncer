$(function () {
    // 初始化标签输入框
    initMultipleInputTags();
    
    // 初始化二维码悬浮提示
    initQRCodePopover({
        url: 'https://work.weixin.qq.com/u/vc7f073c9f993bc776?v=4.1.20.26620',
        selector: '.qrcode-trigger',
        size: 150,
        position: 'top'
    });

    // 删除激活码
    $("#removeBtn").on('click', function(){
        const $btn = $(this);
        showConfirm({
            title: '删除激活码后，产品功能将不可用，确认是否删除？',
            icon: 'warning',
            size: 'large',
            confirmType: 'danger',
            onConfirm: function () {
                // 禁用按钮，防止重复点击
                const originalText = $btn.html();
                $btn.html('<i class="fa fa-spinner fa-spin"></i> 删除中...').prop('disabled', true)
                doPoster("/license/remove", {}, function (response) {
                    $btn.prop('disabled', false);
                    $btn.html(originalText);
                    if (response.success === true) {
                        bootGrowl("删除激活码成功！", "success");
                        doLoader("/license");
                    } else {
                        bootGrowl(response.message || "删除失败", "danger");
                    }
                });
            }
        });
    });

    // 在线激活
    $("#activateBtn").on('click', function(){
        const $form = $("#license-form");
        const $btn = $(this);
        // 防止重复提交
        if ($btn.prop('disabled')) {
            return;
        }
        if (!validateForm($form)) {
            return;
        }

        const originalText = $btn.html();
        $btn.html('<i class="fa fa-spinner fa-spin"></i> 激活中...').prop('disabled', true);
        doPoster("/license/activate", $form.serializeJson(), function (response) {
            $btn.html(originalText).prop('disabled', false);

            if (response.success === true) {
                bootGrowl("在线激活成功！", "success");
                doLoader("/license");
            } else {
                bootGrowl(response.message || "激活失败", "danger");
            }
        });
    });

    // 复制机器码
    $("#copyBtn").on('click', function(){
        const licenseKey = document.getElementById("licenseKey");
        const $btn = $(this);
        
        // 使用现代浏览器的 Clipboard API
        if (navigator.clipboard && window.isSecureContext) {
            navigator.clipboard.writeText(licenseKey.value).then(function() {
                bootGrowl("复制机器码成功！", "success");
                $btn.html('<i class="fa fa-check"></i> 已复制');
                setTimeout(function() {
                    $btn.html('<i class="fa fa-copy"></i> 复制');
                }, 2000);
            }).catch(function(err) {
                console.error('复制失败', err);
                fallbackCopyText(licenseKey.value);
            });
        } else {
            // 降级方案
            fallbackCopyText(licenseKey.value);
        }
    });
    
    // 降级复制方案
    function fallbackCopyText(text) {
        const textArea = document.createElement("textarea");
        textArea.value = text;
        textArea.style.position = 'fixed';
        textArea.style.top = '0';
        textArea.style.left = '0';
        textArea.style.opacity = '0';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
            const successful = document.execCommand('copy');
            if (successful) {
                bootGrowl("复制机器码成功！", "success");
                $("#copyBtn").html('<i class="fa fa-check"></i> 已复制');
                setTimeout(function() {
                    $("#copyBtn").html('<i class="fa fa-copy"></i> 复制');
                }, 2000);
            }
        } catch (err) {
            console.error('复制失败', err);
            bootGrowl("复制失败，请手动复制", "danger");
        }
        document.body.removeChild(textArea);
    }

    // 初始化文件上传组件
    initFileUpload('#licenseUploader', {
        uploadUrl: $basePath + '/license/upload',
        maxFiles: 1,
        maxSize: 10 * 1024 * 1024, // 10MB
        autoUpload: true,
        onSuccess: function(file, response) {
            if (response.success) {
                bootGrowl("激活码上传成功！", "success");
                doLoader("/license");
            } else {
                bootGrowl(response.message || "上传失败", "danger");
            }
        },
        onError: function(file, error) {
            bootGrowl(error || "上传失败", "danger");
        }
    });
});