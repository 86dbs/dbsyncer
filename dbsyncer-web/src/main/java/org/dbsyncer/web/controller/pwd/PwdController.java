package org.dbsyncer.web.controller.pwd;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 密码修改处理
 * @ClassName: PwdController
 * @author AE86
 * @date: 2017年7月7日 上午10:03:33
 */
@Controller
@RequestMapping(value = "/pwd")
public class PwdController extends BaseController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private ConfigService configService;

	@RequestMapping("")
	public String index(ModelMap model) {
		model.put("config", configService.getConfig());
		return "pwd/pwd";
	}

	@PostMapping("/edit")
	@ResponseBody
	public RestResult edit(HttpServletRequest request) {
		try {
			Map<String, String> params = getParams(request);
			return RestResult.restSuccess(configService.edit(params));
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(), e.getClass());
			return RestResult.restFail(e.getMessage());
		}
	}

}
