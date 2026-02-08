/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.vo.RestResult;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

@Controller
@RequestMapping("/index")
public class IndexController {

    @Resource
    private AppConfigService appConfigService;

    @GetMapping("")
    public String index(ModelMap model) {
        return "index/list.html";
    }

    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return RestResult.restSuccess(appConfigService.getVersionInfo(authentication.getName()));
    }
}
