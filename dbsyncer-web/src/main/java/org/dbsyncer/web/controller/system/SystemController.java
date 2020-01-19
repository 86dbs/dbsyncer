package org.dbsyncer.web.controller.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/system")
public class SystemController {

	private final Logger logger = LoggerFactory.getLogger(getClass());
    
	@RequestMapping("")
	public String index(ModelMap model) {
		return "system/system";
	}

}
