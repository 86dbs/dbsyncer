package org.dbsyncer.web.controller.pwd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 密码修改处理
 * @ClassName: PwdController
 * @author AE86
 * @date: 2017年7月7日 上午10:03:33
 */
@Controller
@RequestMapping(value = "/pwd")
public class PwdController {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	@RequestMapping("")
	public String index(ModelMap model) {
		return "pwd/pwd";
	}

	
}
