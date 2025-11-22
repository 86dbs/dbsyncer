/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.task;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.ConnectorServiceContext;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 任务配置管理
 *
 * @Author wuji
 * @Version 1.0.0
 * @Date 2025-10-18 19:52
 */
@Controller
@RequestMapping("/task")
public class TaskController extends BaseController {

    private static final Logger log = LoggerFactory.getLogger(TaskController.class);

    @Resource
    private TaskService taskService;

    @Resource
    private ConnectorService connectorService;

    @Resource
    private ConnectorFactory connectorFactory;

    /**
     * 任务配置首页
     */
    @RequestMapping("")
    public String index(ModelMap model) {
        return "task/list";
    }

    /**
     * 添加任务页面
     */
    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "task/add";
    }

    /**
     * 添加任务
     */
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(@RequestBody Map<String, String> params) {
        try {
            // 设置时间戳
            params.put("createTime", DateFormatUtil.dateToString(new Date()));
            params.put("updateTime", DateFormatUtil.dateToString(new Date()));
            taskService.add(params);
            return RestResult.restSuccess("新增成功");
        } catch (Exception e) {
            log.error("新增任务失败", e);
            return RestResult.restFail("新增失败: " + e.getMessage());
        }
    }

    /**
     * 修改任务
     */
    @PostMapping("/modify")
    @ResponseBody
    public RestResult modify(@RequestBody Map<String, String> params) {
        try {
            // 设置更新时间
            params.put("updateTime", DateFormatUtil.dateToString(new Date()));
            taskService.modify(params);
            return RestResult.restSuccess("修改成功");
        } catch (Exception e) {
            log.error("修改任务失败", e);
            return RestResult.restFail("修改失败: " + e.getMessage());
        }
    }

    /**
     * 删除任务
     */
    @GetMapping("/delete")
    @ResponseBody
    public RestResult delete(String taskId) {
        try {
            taskService.delete(taskId);
            return RestResult.restSuccess("删除成功");
        } catch (Exception e) {
            log.error("删除任务失败", e);
            return RestResult.restFail("删除失败: " + e.getMessage());
        }
    }

    /**
     * 任务列表
     */
    @PostMapping("/list")
    @ResponseBody
    public Paging list(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            Paging list = taskService.list(params);
            return list;

        } catch (Exception e) {
            log.error("查询任务列表失败", e);
            return new Paging(0, 0);
        }
    }

    /**
     * 任务执行结果详情
     */
    @PostMapping("/result")
    @ResponseBody
    public Paging result(@RequestBody Map<String, String> params) {
        try {
            return taskService.result(params);
        } catch (Exception e) {
            log.error("查询任务结果失败", e);
            return new Paging(0, 0);
        }
    }

    /**
     * 获取连接器的数据库列表
     */
    @GetMapping("/getDatabases")
    @ResponseBody
    public RestResult getDatabases(@RequestParam("connectorId") String connectorId) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }
            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), connector.getConfig());
            List<String> databases = getDatabaseList(connectorInstance);

            return RestResult.restSuccess(databases);
        } catch (Exception e) {
            log.error("获取数据库列表失败", e);
            return RestResult.restFail("获取数据库列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取连接器的表列表
     */
    @GetMapping("/getTables")
    @ResponseBody
    public RestResult getTables(@RequestParam("connectorId") String connectorId, @RequestParam(value = "database", required = false) String database, @RequestParam(value = "schema", required = false) String schema) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }

            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), connector.getConfig());
            ConnectorServiceContext context = new DefaultConnectorServiceContext(database, schema, StringUtil.EMPTY);
            List<Table> tables = connectorFactory.getTable(connectorInstance, context);
            return RestResult.restSuccess(tables);
        } catch (Exception e) {
            log.error("获取表列表失败", e);
            return RestResult.restFail("获取表列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取表的字段列表
     */
    @GetMapping("/getTableFields")
    @ResponseBody
    public RestResult getTableFields(@RequestParam("connectorId") String connectorId, @RequestParam(value = "database", required = false) String database, @RequestParam(value = "schema", required = false) String schema, @RequestParam("tableName") String tableName) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }

            ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), connector.getConfig());
            ConnectorServiceContext context = new DefaultConnectorServiceContext(database, schema, tableName);
            List<MetaInfo> metaInfos = connectorFactory.getMetaInfo(connectorInstance, context);
            MetaInfo metaInfo = CollectionUtils.isEmpty(metaInfos) ? null : metaInfos.get(0);
            if (metaInfo == null || metaInfo.getColumn() == null) {
                return RestResult.restFail("获取表字段失败");
            }

            return RestResult.restSuccess(metaInfo.getColumn());
        } catch (Exception e) {
            log.error("获取表字段失败", e);
            return RestResult.restFail("获取表字段失败: " + e.getMessage());
        }
    }

    /**
     * 获取连接器类型
     */
    @GetMapping("/getConnectorType")
    @ResponseBody
    public RestResult getConnectorType(@RequestParam("connectorId") String connectorId) {
        try {
            Connector connector = connectorService.getConnector(connectorId);
            if (connector == null) {
                return RestResult.restFail("连接器不存在");
            }

            return RestResult.restSuccess(connector.getConfig().getConnectorType());
        } catch (Exception e) {
            log.error("获取连接器类型失败", e);
            return RestResult.restFail("获取连接器类型失败: " + e.getMessage());
        }
    }

    /**
     * 获取数据库列表
     */
    private List<String> getDatabaseList(ConnectorInstance connectorInstance) {
        List<String> databases = new ArrayList<>();
        try {
            // 根据连接器类型获取数据库列表
            String connectorType = connectorInstance.getConfig().getConnectorType().toLowerCase();

            DatabaseConnectorInstance connection = (DatabaseConnectorInstance) connectorInstance.getConnection();
            if (connectorType.contains("mysql")) {
                // MySQL: SHOW DATABASES
                databases = connection.execute(databaseTemplate -> {
                    List<String> dbList = new ArrayList<>();
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery("SHOW DATABASES")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            // 过滤系统数据库
                            if (!isSystemDatabase(dbName)) {
                                dbList.add(dbName);
                            }
                        }
                    }
                    return dbList;
                });
            } else if (connectorType.contains("postgresql")) {
                // PostgreSQL: SELECT datname FROM pg_database
                databases = connection.execute(databaseTemplate -> {
                    List<String> dbList = new ArrayList<>();
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            if (!isSystemDatabase(dbName)) {
                                dbList.add(dbName);
                            }
                        }
                    }
                    return dbList;
                });
            } else if (connectorType.contains("oracle")) {
                // Oracle: SELECT username FROM all_users
                databases = connection.execute(databaseTemplate -> {
                    List<String> dbList = new ArrayList<>();
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery("SELECT username FROM all_users ORDER BY username")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            if (!isSystemDatabase(dbName)) {
                                dbList.add(dbName);
                            }
                        }
                    }
                    return dbList;
                });
            } else if (connectorType.contains("sqlserver")) {
                // SQL Server: SELECT name FROM sys.databases
                databases = connection.execute(databaseTemplate -> {
                    List<String> dbList = new ArrayList<>();
                    try (ResultSet rs = databaseTemplate.getSimpleConnection().getConnection().createStatement().executeQuery("SELECT name FROM sys.databases WHERE database_id > 4")) {
                        while (rs.next()) {
                            String dbName = rs.getString(1);
                            if (!isSystemDatabase(dbName)) {
                                dbList.add(dbName);
                            }
                        }
                    }
                    return dbList;
                });
            } else {
                // 其他数据库类型，返回默认数据库
                databases.add("default_database");
            }

        } catch (Exception e) {
            log.error("获取数据库列表异常", e);
            databases.add("default_database");
        }
        return databases;
    }

    /**
     * 判断是否为系统数据库
     */
    private boolean isSystemDatabase(String dbName) {
        if (dbName == null) return true;

        String lowerDbName = dbName.toLowerCase();
        return lowerDbName.equals("information_schema") || lowerDbName.equals("mysql") || lowerDbName.equals("performance_schema") || lowerDbName.equals("sys") || lowerDbName.equals("postgres") || lowerDbName.equals("template0") || lowerDbName.equals("template1") || lowerDbName.equals("master") || lowerDbName.equals("tempdb") || lowerDbName.equals("model") || lowerDbName.equals("msdb");
    }

}
