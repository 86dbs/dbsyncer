
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
	let errorClassName = "dbsyncerVerifcateError";
	if ($this.val() == "") {
		$this.addClass(errorClassName).attr("data-original-title", "必填").tooltip({trigger : 'manual'}).tooltip('show');
		return false;
	}
	// 数字类型校验
	if ($this.attr("type") == "number") {
		let max = parseInt($this.attr("max"));
		let min = parseInt($this.attr("min"));
		if($this.val() > max || $this.val() < min){
			$this.addClass(errorClassName).attr("data-original-title", "有效范围应在" + min + "-" + max).tooltip({trigger: 'manual'}).tooltip('show');
			return false;
		}
	}
	$this.tooltip('hide').removeClass(errorClassName);
	return true;
}