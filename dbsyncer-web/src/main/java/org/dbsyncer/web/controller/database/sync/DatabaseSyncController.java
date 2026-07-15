/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.database.sync;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.DatabaseSyncService;
import org.dbsyncer.biz.vo.DatabaseSyncTaskVO;
import org.dbsyncer.biz.vo.EditionInfoVO;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.sdk.spi.LicenseService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * 整库迁移
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
@Controller
@RequestMapping("/database-sync")
public class DatabaseSyncController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private DatabaseSyncService databaseSyncService;

    @Resource
    private LicenseService licenseService;

    @RequestMapping("/list")
    public String list(ModelMap model) {
        initEditionInfo(model);
        return "database-sync/list";
    }

    private void initEditionInfo(ModelMap model) {
        EditionInfoVO editionInfo = new EditionInfoVO();
        editionInfo.setEdition(licenseService.getEditionEnum().getCode());
        editionInfo.setEditionName(licenseService.getEditionEnum().getMessage());
        model.put("editionInfo", editionInfo);
    }

    /**
     * 添加任务页面
     */
    @GetMapping("/pageAdd")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorRelation());
        model.put("readOnly", false);
        return "database-sync/add";
    }

    /**
     * 任务子页面（与订正校验一致：/page/{page}?id=xxx）
     */
    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam("id") String id,
                      @RequestParam(value = "detailStatus", required = false) String detailStatus,
                      @RequestParam(value = "detailType", required = false) String detailType) {
        model.put("connectors", connectorService.getConnectorAll());
        if ("detail".equals(page)) {
            Assert.hasText(id, "任务 ID 不能为空");
            model.put("taskId", id);
            model.put("taskList", databaseSyncService.getAll());
            model.put("detailStatus", detailStatus == null ? "" : detailStatus.trim());
            model.put("detailType", detailType == null ? "" : detailType.trim());
        } else if ("edit".equals(page)) {
            Assert.hasText(id, "任务 ID 不能为空");
            DatabaseSyncTaskVO task = databaseSyncService.get(id);
            model.put("task", task);
            int status = task.getStatus() != null ? task.getStatus() : CommonTaskStatusEnum.READY.getCode();
            model.put("readOnly", status != CommonTaskStatusEnum.READY.getCode());
        }
        return "database-sync/" + page;
    }

    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(databaseSyncService.search(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(databaseSyncService.add(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(databaseSyncService.edit(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(databaseSyncService.delete(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(databaseSyncService.start(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(databaseSyncService.stop(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 预览连接器下表列表（新增页树形选择，滚动分页；offset/limit 由前端传入）
     */
    @PostMapping("/searchResult")
    @ResponseBody
    public RestResult searchResult(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(databaseSyncService.searchResult(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/previewTables")
    @ResponseBody
    public RestResult previewTables(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(databaseSyncService.previewTables(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
