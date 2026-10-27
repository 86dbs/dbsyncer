/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.log;

import org.dbsyncer.biz.MonitorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.log.OperLog;
import org.dbsyncer.web.log.OperLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 登录日志 / 操作日志。
 *
 * @author wuji
 * @version 1.0.0
 */
@Controller
@RequestMapping("/log")
public class LogController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private MonitorService monitorService;

    @GetMapping("/login")
    public String loginPage(ModelMap model) {
        requirePermission("system:view");
        return "log/login";
    }

    @GetMapping("/oper")
    public String operPage(ModelMap model) {
        requirePermission("system:view");
        model.put("logModules", LogType.listOperModuleNames());
        return "log/oper";
    }

    @PostMapping("/login/query")
    @ResponseBody
    public RestResult queryLogin(HttpServletRequest request) {
        try {
            requirePermission("system:view");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryLoginLog(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/oper/query")
    @ResponseBody
    public RestResult queryOper(HttpServletRequest request) {
        try {
            requirePermission("system:view");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(monitorService.queryOperLog(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/detail")
    @ResponseBody
    public RestResult detail(@RequestParam("id") String id) {
        try {
            requirePermission("system:view");
            return RestResult.restSuccess(monitorService.getLogDetail(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "清空日志")
    @PostMapping("/clear")
    @ResponseBody
    public RestResult clear() {
        try {
            requirePermission("system:edit");
            return RestResult.restSuccess(monitorService.clearLog());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
