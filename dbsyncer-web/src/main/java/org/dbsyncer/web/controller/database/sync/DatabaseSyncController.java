/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.database.sync;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.enums.TableTypeEnum;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 整库迁移
 *
 * @author wuji
 */
@Controller
@RequestMapping("/database-syncer")
public class DatabaseSyncController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ProfileComponent profileComponent;

    @RequestMapping("/list")
    public String list(ModelMap model) {
        return "database-syncer/list";
    }

    @GetMapping("/pageAdd")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "database-syncer/add";
    }

    /**
     * 任务列表（占位：后续对接 TaskService + DatabaseSyncTask）
     */
    @PostMapping("/search")
    @ResponseBody
    public RestResult search(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            int pageNum = NumberUtil.toInt(params.get("pageNum"), 1);
            int pageSize = NumberUtil.toInt(params.get("pageSize"), 10);
            Paging<Map<String, Object>> paging = new Paging<>(pageNum, pageSize);
            paging.setTotal(0);
            paging.setData(Collections.emptyList());
            return RestResult.restSuccess(paging);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 保存任务（占位：先接收前端提交的映射配置 JSON）
     */
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String name = params.get("name");
            if (StringUtil.isBlank(name)) {
                return RestResult.restFail("任务名称不能为空");
            }
            logger.info("整库迁移任务待保存: name={}, mappings={}", name, params.get("databaseMappingsJson"));
            return RestResult.restSuccess("保存成功（后端持久化开发中）");
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
            Map<String, String> params = getParams(request);
            String connectorId = params.get("connectorId");
            String database = params.get("database");
            String schema = params.get("schema");
            String searchKey = StringUtil.trimToEmpty(params.get("searchKey"));
            int offset = Math.max(0, NumberUtil.toInt(params.get("offset"), 0));
            int limit = NumberUtil.toInt(params.get("limit"), 0);
            if (limit <= 0) {
                return RestResult.restFail("limit 必须大于 0");
            }
            if (StringUtil.isBlank(connectorId)) {
                return RestResult.restFail("连接器不能为空");
            }
            Connector connector = profileComponent.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }
            DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(
                    "database-syncer-preview",
                    connectorId, database, schema,
                    connectorId, database, schema,
                    true);
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId());
            List<Table> tables = connectorFactory.getTables(connectorInstance, context);
            if (CollectionUtils.isEmpty(tables)) {
                return RestResult.restSuccess(buildPreviewTablesResult(Collections.emptyList(), 0, offset, limit));
            }
            if (StringUtil.isNotBlank(searchKey)) {
                String key = searchKey.toUpperCase();
                tables = tables.stream()
                        .filter(t -> t.getName() != null && t.getName().toUpperCase().contains(key))
                        .collect(Collectors.toList());
            }
            tables.sort(Comparator.comparing(Table::getName, String.CASE_INSENSITIVE_ORDER));
            int realTotal = tables.size();
            Map<String, Integer> typeCounts = new HashMap<>(4);
            for (Table table : tables) {
                String type = table.getType() != null ? table.getType() : TableTypeEnum.TABLE.getCode();
                typeCounts.merge(type.toUpperCase(), 1, Integer::sum);
            }
            int from = Math.min(offset, realTotal);
            int to = Math.min(from + limit, realTotal);
            List<Map<String, Object>> pageRows = new ArrayList<>();
            for (int i = from; i < to; i++) {
                Table table = tables.get(i);
                Map<String, Object> row = new HashMap<>(4);
                row.put("name", table.getName());
                row.put("type", table.getType() != null ? table.getType() : TableTypeEnum.TABLE.getCode());
                pageRows.add(row);
            }
            Map<String, Object> result = buildPreviewTablesResult(pageRows, realTotal, offset, limit);
            result.put("typeCounts", typeCounts);
            return RestResult.restSuccess(result);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private Map<String, Object> buildPreviewTablesResult(List<Map<String, Object>> data, int total, int offset, int limit) {
        Map<String, Object> result = new HashMap<>(8);
        result.put("data", data);
        result.put("total", total);
        result.put("offset", offset);
        result.put("limit", limit);
        int from = Math.min(offset, total);
        int to = Math.min(from + limit, total);
        result.put("hasMore", to < total);
        return result;
    }

}
