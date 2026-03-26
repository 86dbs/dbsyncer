package org.dbsyncer.web.controller;

import org.springframework.boot.web.server.ErrorPage;
import org.springframework.boot.web.server.ErrorPageRegistrar;
import org.springframework.boot.web.server.ErrorPageRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/")
public class DefaultController implements ErrorPageRegistrar {

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        return "index.html";
    }

    @RequestMapping({"/login", "/login.html"})
    public String index(ModelMap model) {
        return "login.html";
    }

    @Override
    public void registerErrorPages(ErrorPageRegistry registry) {
        registry.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/404.html"));
    }
}
