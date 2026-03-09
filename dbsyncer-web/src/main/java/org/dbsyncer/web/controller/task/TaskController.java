/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.task;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 订正校验管理
 *
 * @Author wuji
 * @Version 1.0.0
 * @Date 2025-10-18 19:52
 */
@Controller
@RequestMapping("/validate-sync")
public class TaskController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskService taskService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private ConnectorFactory connectorFactory;

    /**
     * 任务配置首页
     */
    @RequestMapping("/list")
    public String index(ModelMap model) {
        // TODO 修改命名
        return "task/list";
    }

    /**
     * 添加任务页面
     */
    @GetMapping("/pageAdd")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "task/add";
    }

    /**
     * 编辑任务页面
     */
    @GetMapping("/page/{page}")
    public String pageEdit(ModelMap model, @PathVariable("page") String page, @RequestParam("id") String taskId) {
        model.put("taskId", taskId);
        return "task/" + page;
    }

    /**
     * 添加任务
     */
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(@RequestBody Map<String, String> params) {
        try {
            taskService.add(params);
            return RestResult.restSuccess("新增成功");
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
    public RestResult edit(@RequestBody Map<String, String> params) {
        try {
            taskService.modify(params);
            return RestResult.restSuccess("修改成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 删除任务
     */
    @GetMapping("/remove")
    @ResponseBody
    public RestResult remove(String taskId) {
        try {
            taskService.delete(taskId);
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
    public RestResult start(@RequestParam("taskId") String taskId) {
        try {
            taskService.start(taskId);
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
    public RestResult stop(@RequestParam("taskId") String taskId) {
        try {
            taskService.stop(taskId);
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
            return RestResult.restSuccess(taskService.list(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 任务执行结果详情
     */
    @PostMapping("/result")
    @ResponseBody
    public RestResult result(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(taskService.result(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}