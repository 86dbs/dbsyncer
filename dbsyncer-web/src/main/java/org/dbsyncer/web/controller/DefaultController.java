package org.dbsyncer.web.controller;

import org.dbsyncer.biz.SystemConfigService;
import org.springframework.boot.web.server.ErrorPage;
import org.springframework.boot.web.server.ErrorPageRegistrar;
import org.springframework.boot.web.server.ErrorPageRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/")
public class DefaultController implements ErrorPageRegistrar {

    @Resource
    private SystemConfigService systemConfigService;

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        model.put("enableCDN", systemConfigService.isEnableCDN());
        return "index.html";
    }

    @RequestMapping({"/login", "/login.html"})
    public String index(ModelMap model) {
        model.put("enableCDN", systemConfigService.isEnableCDN());
        return "login.html";
    }

    @Override
    public void registerErrorPages(ErrorPageRegistry registry) {
        registry.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/404.html"));
    }
}