package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/monitor")
public class MonitorController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorService monitorService;

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        model.put("threadInfo", monitorService.getThreadInfo());
        return "monitor/monitor.html";
    }

    @GetMapping("/queryData")
    @ResponseBody
    public RestResult queryData(@RequestParam(value = "id") String id, @RequestParam(value = "pageNum") int pageNum, @RequestParam(value = "pageSize") int pageSize) {
        try {
            return RestResult.restSuccess(monitorService.queryData(id, pageNum, pageSize));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/queryLog")
    @ResponseBody
    public RestResult queryLog(@RequestParam(value = "type") String type, @RequestParam(value = "pageNum") int pageNum, @RequestParam(value = "pageSize") int pageSize) {
        try {
            return RestResult.restSuccess(monitorService.queryLog(type, pageNum, pageSize));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/clearData")
    @ResponseBody
    public RestResult clearData(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(monitorService.clearData(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/clearLog")
    @ResponseBody
    public RestResult clearLog(@RequestParam(value = "type") String type) {
        try {
            return RestResult.restSuccess(monitorService.clearLog(type));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}