/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.validate.sync;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 订正校验管理
 *
 * @author wuji
 * @version 1.0.0
 * @date 2025-10-18 19:52
 */
@Controller
@RequestMapping("/validate-sync")
public class ValidateSyncController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskService taskService;

    @Resource
    private ConnectorService connectorService;

    /**
     * 任务配置首页
     */
    @RequestMapping("/list")
    public String index(ModelMap model) {
        // TODO 修改命名
        return "validate-sync/list";
    }

    /**
     * 添加任务页面
     */
    @GetMapping("/pageAdd")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "validate-sync/add";
    }

    /**
     * 修改任务页面
     */
    @GetMapping("/page/{page}")
    public String pageEdit(ModelMap model, @PathVariable("page") String page, @RequestParam("id") String id) {
        model.put("task", taskService.get(id));
        return "validate-sync/" + page;
    }

    /**
     * 添加任务
     */
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(taskService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 修改任务
     */
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(taskService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 删除任务
     */
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            taskService.delete(id);
            return RestResult.restSuccess("删除成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 启动任务
     */
    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam(value = "id") String id) {
        try {
            taskService.start(id);
            return RestResult.restSuccess("启动成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 停止任务
     */
    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam(value = "id") String id) {
        try {
            taskService.stop(id);
            return RestResult.restSuccess("停止成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult list(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(taskService.search(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 查看执行结果
     */
    @PostMapping("/result")
    @ResponseBody
    public RestResult result(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(taskService.result(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}