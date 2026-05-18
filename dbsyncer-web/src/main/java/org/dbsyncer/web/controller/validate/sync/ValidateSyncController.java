/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.validate.sync;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

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
    private ValidateSyncService validateSyncService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private TableGroupService tableGroupService;

    /**
     * 任务配置首页
     */
    @RequestMapping("/list")
    public String index(ModelMap model) {
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
        if (page.equals("detail")) {
            model.put("taskId", id);
            model.put("taskList", validateSyncService.getAll());
        } else if (page.equals("editTableGroup")) {
            TableGroup tableGroup = tableGroupService.getTableGroup(id);
            model.put("tableGroup", tableGroup);
            model.put("task", validateSyncService.get(tableGroup.getMappingId()));
        } else {
            model.put("task", validateSyncService.get(id));
        }
        initConditionConfig(model);
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
            return RestResult.restSuccess(validateSyncService.add(params));
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
            return RestResult.restSuccess(validateSyncService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(validateSyncService.copy(id));
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
            return RestResult.restSuccess(validateSyncService.delete(id));
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
            return RestResult.restSuccess(validateSyncService.start(id));
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
            return RestResult.restSuccess(validateSyncService.stop(id));
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
            return RestResult.restSuccess(validateSyncService.search(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/searchTableGroup")
    @ResponseBody
    public RestResult searchTableGroup(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(validateSyncService.searchTableGroup(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/refreshTables")
    @ResponseBody
    public RestResult refreshTables(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(validateSyncService.refreshTables(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 分页搜索校验任务表（用于下拉框远程搜索）
     */
    @PostMapping("/searchTables")
    @ResponseBody
    public RestResult searchTables(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(validateSyncService.searchTables(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshFields")
    @ResponseBody
    public RestResult refreshFields(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(validateSyncService.refreshFields(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/addTableGroup")
    @ResponseBody
    public RestResult addTableGroup(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(validateSyncService.addTableGroup(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/editTableGroup")
    @ResponseBody
    public RestResult editTableGroup(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(validateSyncService.editTableGroup(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/removeTableGroup")
    @ResponseBody
    public RestResult removeTableGroup(
            @RequestParam(value = "taskId", required = false) String taskId,
            @RequestParam(value = "ids") String ids) {
        try {
            return RestResult.restSuccess(validateSyncService.removeTableGroup(taskId, ids));
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
            return RestResult.restSuccess(validateSyncService.result(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 分页查询校验结果明细
     */
    @PostMapping("/searchResult")
    @ResponseBody
    public RestResult searchResult(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(validateSyncService.searchResult(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 按明细 ID 查询单条校验结果（含完整 CONTENT），供详情弹窗使用。
     */
    @PostMapping("/getResultDetail")
    @ResponseBody
    public RestResult getResultDetail(@RequestParam("id") String id) {
        try {
            Object detail = validateSyncService.getValidateResultDetail(id);
            if (detail == null) {
                return RestResult.restFail("记录不存在", 404);
            }
            return RestResult.restSuccess(detail);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}