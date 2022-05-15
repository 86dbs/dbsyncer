package org.dbsyncer.web.controller.config;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
@RequestMapping("/config")
public class ConfigController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConfigService configService;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("config", configService.getConfigModelAll());
        return "config/config";
    }

    @PostMapping(value = "/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload() {
        try {
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response) {

    }

}