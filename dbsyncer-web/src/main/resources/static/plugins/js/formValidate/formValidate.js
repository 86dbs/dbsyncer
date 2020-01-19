
//******************* 验证信息 ***************************
//校验表单信息
$.fn.formValidate = function(opt) {
	var $self = $(this);
	var passValid = true;
	var require = $self.find('[dbsyncer-valid="require"]');

	// 验证必填
	require.each(function() {
		//如果为空，则添加样式
		if (!formValidateMethod($(this))) {
			passValid = false;
		}
	}).on('keyup', function() {
		formValidateMethod($(this));
	}).on('focus', function() {
		formValidateMethod($(this));
	});
	
	// 如果验证不成功
	return passValid;
}

var formValidateMethod = function($this){
	if ($this.val() == "") {
		$this.addClass("dbsyncerVerifcateError").attr("data-original-title", "必填").tooltip({trigger : 'manual'}).tooltip('show');
		return false;
	}
	$this.tooltip('hide').removeClass("dbsyncerVerifcateError");
	return true;
}
