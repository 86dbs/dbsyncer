package org.dbsyncer.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/")
public class DefaultController {

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        return "index.html";
    }

    @RequestMapping({"/login", "/login.html"})
    public String index(ModelMap model) {
        return "login.html";
    }

}