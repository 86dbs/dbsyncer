$(function () {
    // 绑定多值输入框事件
    initMultipleInputTags();
    // 刷新授权信息
    refreshLicenseInfo();

    new QRCode("qrcode", {
        text: "https://work.weixin.qq.com/u/vc7f073c9f993bc776?v=4.1.20.26620",
        width: 200,
        height: 200
    });
    let $myService = $("#myService");
    $myService.hover(function () {
        if(!$myService.attr('init') == true){
            $myService.attr("init", true);
            $myService.unbind('mouseenter').unbind('mouseleave');
            $myService.popover({
                title: "<span class='fa fa-wechat'></span> 微信扫码",
                trigger: 'hover',
                placement: 'bottom',
                html: 'true',
                content: "<img src='" + $("#qrcode").find("img:first").attr('src') + "' />"
            }).on('shown.bs.popover', function (event) {
                const that = this;
                $(this).parent().find('div.popover').on('mouseenter', function () {
                    $(that).attr('in', true);
                }).on('mouseleave', function () {
                    $(that).removeAttr('in');
                    $(that).popover('hide');
                });
            }).on('hide.bs.popover', function (event) {
                if ($(this).attr('in')) {
                    event.preventDefault();
                }
            });
            $myService.popover('show');
        }
    })

    // 删除激活码
    $("#removeBtn").bind('click', function(){
        // 如果当前为恢复状态
        BootstrapDialog.show({
            title: "警告",
            type: BootstrapDialog.TYPE_DANGER,
            message: "删除激活码后，产品功能将不可用，确认是否删除？",
            size: BootstrapDialog.SIZE_NORMAL,
            buttons: [{
                label: "确定",
                action: function (dialog) {
                    doPoster("/license/remove", {}, function (data) {
                        if (data.success == true) {
                            bootGrowl("删除激活码成功！", "success");
                            doLoader("/license");
                        } else {
                            bootGrowl(data.resultValue, "danger");
                        }
                    });
                    dialog.close();
                }
            }, {
                label: "取消",
                action: function (dialog) {
                    dialog.close();
                }
            }]
        });
    });

    // 在线激活
    $("#activateBtn").bind('click', function(){
        const $form = $("#licenseForm");
        if ($form.formValidate() == true) {
            const data = $form.serializeJson();
            doPoster("/license/activate", data, function (data) {
                if (data.success == true) {
                    bootGrowl("在线激活成功！", "success");
                    doLoader("/license");
                } else {
                    bootGrowl(data.resultValue, "danger");
                }
            });
        }
    });

    $("#copyBtn").bind('click', function(){
        //Get the copied text
        let textArea = document.createElement("textarea");
        textArea.value = document.getElementById("licenseKey").value;
        textArea.style.height='0px';
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
            let successful = document.execCommand('copy');
            if (successful) {
                bootGrowl("复制机器码成功！", "success");
            }
        } catch (err) {
            console.error('复制失败', err);
        }
        document.body.removeChild(textArea);
    });

    $("#fileLicense").fileinput({
        theme: 'fas',
        language: 'zh',
        uploadUrl: $basePath + '/license/upload',
        enctype: 'multipart/form-data',
        removeFromPreviewOnError: true, //当选择的文件不符合规则时，例如不是指定后缀文件、大小超出配置等，选择的文件不会出现在预览框中，只会显示错误信息
        minFileCount: 1, //每次多次上载允许的最小文件数。如果设置为0，则表示文件数是可选的
        maxFileCount: 1, //表示允许同时上传的最大文件个数 如果设置为0，则表示允许的文件数不受限制
        showUpload: true,//不展示上传按钮
        validateInitialCount: true,//是否在验证minFileCount和包含初始预览文件计数（服务器上载文件）maxFileCount
    }).on("fileuploaded", function (event, data, previewId, index) {
        if (!data.response.success) {
            bootGrowl(data.response.resultValue, "danger");
        }
        doLoader("/license");
    });

});