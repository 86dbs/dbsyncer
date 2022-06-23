/*****************************************************
 *                                                 *#*
 * @项目:loadingT                                   *#*
 * @作者:AE86                                       *#*
 * @Date:2016-11-21 23:46:56                       *#*
 *****************************************************
 *###################################################*/

//定义loadingT初始配置
$.loadingT = {
	init: function(){
		//初始化加载
		$("body").append('<div class="loadingT" ></div>');
		var title = '<h4>正在拼命加载...</h4>';
		var content = '<i class="fa fa-spin fa-3x fa-refresh"></i>';
		var $loadingT = $(".loadingT");
		$loadingT.html("<div class='loading-indicator' unselectable='on' onselectstart='return false;'>"+title+content+"</div>");
		// $loadingT.css({ "height":$(document.body).height()+'px' });
		var $indicato = $(".loading-indicator");
		$indicato.css({ "margin-top":($(window).height() / 2) -($indicato.height() / 2) });
	}
}
//创建遮罩层
$.loadingT.init();
//定义遮罩层开启关闭方法
jQuery.extend({
	loadingT:function (a) {
		if(a){ $(".loadingT").fadeIn(300);}else{$(".loadingT").fadeOut(300);}
	},
	resetIndicato:function(){
		// $(".loadingT").css({ "height":$(document.body).height()+'px' });
		var $indicato = $(".loading-indicator");
		$indicato.css({ "margin-top":($(window).height() / 2) -($indicato.height() / 2) });
	}
});
//兼容浏览器缩放
$(window).resize(function() {
	$.resetIndicato();
});
