package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

@Controller
@RequestMapping("/meta")
public class MetaController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MonitorService monitorService;

    @GetMapping("/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess(monitorService.getMetaAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}