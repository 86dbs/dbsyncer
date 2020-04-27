package org.dbsyncer.web.controller.monitor;

import org.dbsyncer.biz.MonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/monitor")
public class MonitorController {

    @Autowired
    private MonitorService monitorService;

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        model.put("threadInfo", monitorService.getThreadInfo());
        return "monitor/monitor.html";
    }

}