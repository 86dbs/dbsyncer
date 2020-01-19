package org.dbsyncer.web.controller;

import org.dbsyncer.web.remote.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/test")
public class TestController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private UserService userService;

    @GetMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        return "test.html";
    }

    @RequestMapping("/demo")
    @ResponseBody
    public Object demo(Model model) {
        logger.info("demo");

        //String res = userService.hello("我是master");
        //logger.info("slave响应:{}", res);
        return "hello";
    }

}