/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.capture;

import org.dbsyncer.biz.CaptureTaskService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.CaptureAgentService;
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
import java.util.List;
import java.util.Map;

/**
 * 采集任务管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
@Controller
@RequestMapping("/capture-task")
public class CaptureTaskController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private CaptureAgentService captureAgentService;
    @Resource
    private CaptureTaskService captureTaskService;

    @GetMapping("/list")
    public String list(ModelMap model) {
        initEditionInfo(model);
        model.put("captureAvailable", captureAgentService.isAvailable());
        return "capture-task/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model, @RequestParam(value = "type", required = false) String type) {
        initEditionInfo(model);
        model.put("captureType", StringUtil.isBlank(type) ? "captureLocal" : type);
        return "capture-task/add";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, @RequestParam("id") String id) {
        initEditionInfo(model);
        Map<String, Object> task = captureTaskService.get(id);
        model.put("task", task);
        model.put("captureType", task == null ? null : task.get("type"));
        return "capture-task/edit";
    }

    @PostMapping("/query")
    @ResponseBody
    public RestResult query() {
        try {
            requirePermission("capture:view");
            List<Map<String, Object>> list = captureTaskService.list();
            Paging<Map<String, Object>> paging = new Paging<>(1, list == null ? 0 : list.size());
            paging.setData(list);
            paging.setTotal(list == null ? 0 : list.size());
            return RestResult.restSuccess(paging);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "新增采集任务")
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            requirePermission("capture:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(captureTaskService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "编辑采集任务")
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            requirePermission("capture:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(captureTaskService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "删除采集任务")
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam("id") String id) {
        try {
            requirePermission("capture:edit");
            captureTaskService.remove(id);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "启动采集任务")
    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam("id") String id) {
        try {
            requirePermission("capture:edit");
            captureAgentService.start(id);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "停止采集任务")
    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam("id") String id) {
        try {
            requirePermission("capture:edit");
            captureAgentService.stop(id);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/generateSecret")
    @ResponseBody
    public RestResult generateSecret() {
        try {
            requirePermission("capture:edit");
            return RestResult.restSuccess(captureTaskService.generateSecret());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
