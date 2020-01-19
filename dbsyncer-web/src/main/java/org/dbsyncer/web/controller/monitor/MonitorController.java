package org.dbsyncer.web.controller.monitor;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/monitor")
public class MonitorController {

    @RequestMapping("")
    public String index(HttpServletRequest request, ModelMap model) {
        return "monitor/monitor.html";
    }

}
