$(function () {
    new QRCode("qrcode", {
        text: "https://work.weixin.qq.com/u/vc7f073c9f993bc776?v=4.1.20.26620",
        width: 200,
        height: 200
    });
    let imgSelector = qrcode.querySelector("img");
    imgSelector.onload = () => {
        let content = "<img src='" + imgSelector.src + "' />";
        $("#myService").popover({
            title: "<span class='fa fa-wechat'></span> 微信扫码",
            trigger: 'hover',
            placement: 'bottom',
            html: 'true',
            content: content
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
    }

    document.getElementById("copyBtn").addEventListener('click', async event => {
        //Get the copied text
        const text = document.getElementById("licenseKey").value;
        fallbackCopyTextToClipboard(text);
    });

    // 旧浏览器的回退方案
    function fallbackCopyTextToClipboard(text) {
        var textArea = document.createElement("textarea");
        textArea.value = text;
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
            var successful = document.execCommand('copy');
            var msg = successful ? '成功' : '失败';
            console.log('复制' + msg);
        } catch (err) {
            console.error('复制失败', err);
        }
        document.body.removeChild(textArea);
    }

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