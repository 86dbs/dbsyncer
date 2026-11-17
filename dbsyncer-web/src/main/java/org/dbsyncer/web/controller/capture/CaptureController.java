/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.capture;

import org.dbsyncer.biz.CaptureTaskService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.spi.CaptureAgentService;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.log.OperLog;
import org.dbsyncer.web.log.OperLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
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
 * 本地采集 / 远端采集管理（页面与权限相互独立）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
@Controller
@RequestMapping("/capture")
public class CaptureController extends BaseController {

    private static final String PERM_LOCAL_VIEW = "captureLocal:view";
    private static final String PERM_LOCAL_EDIT = "captureLocal:edit";
    private static final String PERM_REMOTE_VIEW = "captureRemote:view";
    private static final String PERM_REMOTE_EDIT = "captureRemote:edit";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private CaptureAgentService captureAgentService;
    @Resource
    private CaptureTaskService captureTaskService;

    @GetMapping("/list")
    public String list(ModelMap model) {
        return listLocal(model);
    }

    @GetMapping("/local/list")
    public String listLocal(ModelMap model) {
        return renderList(model, ConfigConstant.CAPTURE_LOCAL);
    }

    @GetMapping("/remote/list")
    public String listRemote(ModelMap model) {
        return renderList(model, ConfigConstant.CAPTURE_REMOTE);
    }

    @GetMapping("/page/add")
    public String pageAddLegacy(ModelMap model, @RequestParam(value = "type", required = false) String type) {
        return ConfigConstant.CAPTURE_REMOTE.equals(StringUtil.trim(type))
                ? pageAddRemote(model)
                : pageAddLocal(model);
    }

    @GetMapping("/local/page/add")
    public String pageAddLocal(ModelMap model) {
        return renderAdd(model, ConfigConstant.CAPTURE_LOCAL);
    }

    @GetMapping("/remote/page/add")
    public String pageAddRemote(ModelMap model) {
        return renderAdd(model, ConfigConstant.CAPTURE_REMOTE);
    }

    @GetMapping("/page/edit")
    public String pageEditLegacy(ModelMap model, @RequestParam("id") String id) {
        Map<String, Object> task = captureTaskService.get(id);
        Assert.notNull(task, "采集任务不存在");
        String type = String.valueOf(task.get("type"));
        return ConfigConstant.CAPTURE_REMOTE.equals(type)
                ? pageEditRemote(model, id)
                : pageEditLocal(model, id);
    }

    @GetMapping("/local/page/edit")
    public String pageEditLocal(ModelMap model, @RequestParam("id") String id) {
        return renderEdit(model, id, ConfigConstant.CAPTURE_LOCAL);
    }

    @GetMapping("/remote/page/edit")
    public String pageEditRemote(ModelMap model, @RequestParam("id") String id) {
        return renderEdit(model, id, ConfigConstant.CAPTURE_REMOTE);
    }

    @PostMapping("/query")
    @ResponseBody
    public RestResult query(@RequestParam(value = "type", required = false) String type) {
        try {
            String normalized = StringUtil.trim(type);
            Assert.hasText(normalized, "采集类型不能为空");
            requirePermission(viewPermission(normalized));
            List<Map<String, Object>> list = captureTaskService.list(normalized);
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
            Map<String, String> params = getParams(request);
            requirePermission(editPermission(params.get("type")));
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
            Map<String, String> params = getParams(request);
            requirePermission(editPermission(resolveType(params.get("type"), params.get("id"))));
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
            requirePermission(editPermission(resolveType(null, id)));
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
            requirePermission(editPermission(resolveType(null, id)));
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
            requirePermission(editPermission(resolveType(null, id)));
            captureAgentService.stop(id);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/generateSecret")
    @ResponseBody
    public RestResult generateSecret(@RequestParam(value = "type", required = false) String type) {
        try {
            String normalized = StringUtil.trim(type);
            if (StringUtil.isNotBlank(normalized)) {
                requirePermission(editPermission(normalized));
            } else {
                requireAnyPermission(PERM_LOCAL_EDIT, PERM_REMOTE_EDIT);
            }
            return RestResult.restSuccess(captureTaskService.generateSecret());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/local/clients")
    @ResponseBody
    public RestResult listLocalClients(@RequestParam("id") String id) {
        try {
            requirePermission(PERM_LOCAL_VIEW);
            Assert.hasText(id, "ID不能为空");
            Map<String, Object> task = captureTaskService.get(id);
            Assert.notNull(task, "采集任务不存在");
            Assert.isTrue(ConfigConstant.CAPTURE_LOCAL.equals(String.valueOf(task.get("type"))),
                    "仅本地采集支持查看客户端");
            return RestResult.restSuccess(captureAgentService.listLocalClients(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private String renderList(ModelMap model, String type) {
        initEditionInfo(model);
        return ConfigConstant.CAPTURE_REMOTE.equals(type) ? "capture/remote/list" : "capture/local/list";
    }

    private String renderAdd(ModelMap model, String type) {
        initEditionInfo(model);
        model.put("captureType", type);
        return ConfigConstant.CAPTURE_REMOTE.equals(type) ? "capture/remote/add" : "capture/local/add";
    }

    private String renderEdit(ModelMap model, String id, String expectedType) {
        initEditionInfo(model);
        Map<String, Object> task = captureTaskService.get(id);
        Assert.notNull(task, "采集任务不存在");
        String actualType = String.valueOf(task.get("type"));
        Assert.isTrue(expectedType.equals(actualType), "采集任务类型不匹配");
        model.put("task", task);
        model.put("captureType", expectedType);
        return ConfigConstant.CAPTURE_REMOTE.equals(expectedType) ? "capture/remote/edit" : "capture/local/edit";
    }

    private String resolveType(String type, String id) {
        String normalized = StringUtil.trim(type);
        if (StringUtil.isNotBlank(normalized)) {
            return normalized;
        }
        Map<String, Object> task = captureTaskService.get(id);
        Assert.notNull(task, "采集任务不存在");
        Object taskType = task.get("type");
        Assert.notNull(taskType, "采集任务类型为空");
        return String.valueOf(taskType);
    }

    private static String viewPermission(String type) {
        return ConfigConstant.CAPTURE_REMOTE.equals(type) ? PERM_REMOTE_VIEW : PERM_LOCAL_VIEW;
    }

    private static String editPermission(String type) {
        return ConfigConstant.CAPTURE_REMOTE.equals(type) ? PERM_REMOTE_EDIT : PERM_LOCAL_EDIT;
    }

}
