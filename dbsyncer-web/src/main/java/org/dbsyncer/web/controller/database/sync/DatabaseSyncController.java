/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.database.sync;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.DatabaseSyncService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.web.controller.BaseController;
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

/**
 * 整库迁移
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-05-22 00:00
 */
@Controller
@RequestMapping("/database-syncer")
public class DatabaseSyncController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private DatabaseSyncService databaseSyncService;

    @RequestMapping("/list")
    public String list(ModelMap model) {
        return "database-syncer/list";
    }

    @GetMapping("/pageAdd")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "database-syncer/add";
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
