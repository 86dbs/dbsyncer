package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/mapping")
public class MappingController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private TableGroupService tableGroupService;


    @Resource
    ConnectorFactory connectorFactory;

    /**
     * 同步任务列表页面
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        model.put("mappings", mappingService.getMappingAll());
        return "mapping/list";
    }

    /**
     * 添加同步任务页面
     */
    @GetMapping("/pageAdd")
    public String page(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        model.put("mappings", mappingService.getMappingAll());
        return "mapping/add";
    }

    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id, Integer exclude) {

        MappingVo mapping = mappingService.getMapping(id, exclude);
        model.put("mapping", mapping);
        model.put("tableGroups", tableGroupService.getTableGroupAll(id));
        model.put("targetTable", tableGroupService.getTableGroupAll(id));
        initConfig(model);
        return "mapping/" + page;
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(mappingService.copy(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(mappingService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(mappingService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.start(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.stop(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshTables")
    @ResponseBody
    public RestResult refreshTables(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.refreshMappingTables(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 获取连接器信息（类型等）
     *
     * @param connectorId 连接器ID
     * @return 连接器信息
     */
    @GetMapping("/getConnectorInfo")
    @ResponseBody
    public RestResult getConnectorInfo(@RequestParam(value = "connectorId") String connectorId) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(connectorId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 获取数据库列表或Schema列表
     *
     * @param connectorId 连接器ID
     * @return 数据库或Schema列表
     */
    @GetMapping("/getDatabaseOrSchemaList")
    @ResponseBody
    public RestResult getDatabaseOrSchemaList(@RequestParam(value = "connectorId") String connectorId) {
        try {
            return RestResult.restSuccess(connectorService.getConnector(connectorId));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 根据连接器和数据库获取Schema列表（用于PostgreSQL/SQL Server级联选择）
     *
     * @param connectorId 连接器ID
     * @param databaseName 数据库名称
     * @return Schema列表
     */
    @GetMapping("/getSchemaList")
    @ResponseBody
    public RestResult getSchemaList(@RequestParam(value = "connectorId") String connectorId,
                                     @RequestParam(value = "databaseName") String databaseName) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }

            String connectorType = connector.getConfig().getConnectorType().toLowerCase();
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getConfig());

            List<String> schemas = new ArrayList<>();

            if (!(connectorInstance instanceof DatabaseConnectorInstance)) {
                return RestResult.restSuccess(schemas);
            }

            DatabaseConnectorInstance connection = (DatabaseConnectorInstance) connectorInstance;

            if (connectorType.contains("postgresql")) {
                // PostgreSQL
                schemas = connection.execute(databaseTemplate -> {
                    List<String> schemaList = new ArrayList<>();
                    String sql = String.format(
                        "SELECT schema_name FROM information_schema.schemata " +
                        "WHERE catalog_name = '%s' AND schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')",
                        databaseName
                    );
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery(sql)) {
                        while (rs.next()) {
                            schemaList.add(rs.getString(1));
                        }
                    }
                    return schemaList;
                });
            } else if (connectorType.contains("sqlserver")) {
                // SQL Server
                schemas = connection.execute(databaseTemplate -> {
                    List<String> schemaList = new ArrayList<>();
                    String sql = String.format(
                        "SELECT name FROM [%s].sys.schemas " +
                        "WHERE name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', " +
                        "'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')",
                        databaseName
                    );
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery(sql)) {
                        while (rs.next()) {
                            schemaList.add(rs.getString(1));
                        }
                    }
                    return schemaList;
                });
            }

            return RestResult.restSuccess(schemas);
        } catch (Exception e) {
            logger.error("获取Schema列表失败", e);
            return RestResult.restFail("获取Schema列表失败: " + e.getMessage());
        }
    }

}