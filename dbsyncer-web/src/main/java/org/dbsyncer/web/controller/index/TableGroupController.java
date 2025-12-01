package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
@RequestMapping("/tableGroup")
public class TableGroupController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private MappingService mappingService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private ProfileComponent profileComponent;


    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id) throws Exception {
        TableGroup tableGroup = tableGroupService.getTableGroup(id);
        model.put("tableGroup", tableGroup);
        String mappingId = tableGroup.getMappingId();
        model.put("mapping", mappingService.getMapping(mappingId));
        initConfig(model);
        return "mapping/" + page;
    }

    @PostMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(tableGroupService.add(params));
        } catch (SdkException e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage(), 400);
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
            return RestResult.restSuccess(tableGroupService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshFields")
    @ResponseBody
    public RestResult refreshFields(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(tableGroupService.refreshFields(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "mappingId") String mappingId, @RequestParam(value = "ids") String ids) {
        try {
            return RestResult.restSuccess(tableGroupService.remove(mappingId, ids));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/createTargetTable")
    @ResponseBody
    public RestResult createTargetTable(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            String mappingId = params.get("mappingId");
            String sourceTable = params.get("sourceTable");
            String targetTable = params.get("targetTable");

            Assert.hasText(mappingId, "mappingId 不能为空");
            Assert.hasText(sourceTable, "sourceTable 不能为空");
            Assert.hasText(targetTable, "targetTable 不能为空");

            Mapping mapping = profileComponent.getMapping(mappingId);
            Assert.notNull(mapping, "Mapping 不存在: " + mappingId);

            // 获取连接器配置
            Connector sourceConnector = profileComponent.getConnector(mapping.getSourceConnectorId());
            Connector targetConnector = profileComponent.getConnector(mapping.getTargetConnectorId());
            Assert.notNull(sourceConnector, "源连接器不存在: " + mapping.getSourceConnectorId());
            Assert.notNull(targetConnector, "目标连接器不存在: " + mapping.getTargetConnectorId());

            // 连接源和目标数据库
            ConnectorInstance sourceConnectorInstance = connectorFactory.connect(sourceConnector.getConfig());
            ConnectorInstance targetConnectorInstance = connectorFactory.connect(targetConnector.getConfig());

            // 检查目标表是否已存在（避免重复创建）
            try {
                MetaInfo existingTable = connectorFactory.getMetaInfo(targetConnectorInstance, targetTable);
                if (existingTable != null && existingTable.getColumn() != null && !existingTable.getColumn().isEmpty()) {
                    return RestResult.restSuccess("目标表已存在，无需创建");
                }
            } catch (Exception e) {
                // 表不存在，继续创建流程
                logger.debug("目标表不存在，开始创建: {}", targetTable);
            }

            // 获取源表结构
            MetaInfo sourceMetaInfo = connectorFactory.getMetaInfo(sourceConnectorInstance, sourceTable);
            Assert.notNull(sourceMetaInfo, "无法获取源表结构: " + sourceTable);
            Assert.notEmpty(sourceMetaInfo.getColumn(), "源表没有字段: " + sourceTable);

            // 检查连接器是否支持生成 CREATE TABLE DDL
            ConnectorService targetConnectorService = connectorFactory.getConnectorService(targetConnector.getConfig().getConnectorType());
            try {
                // 尝试生成 CREATE TABLE DDL
                String createTableDDL = targetConnectorService.generateCreateTableDDL(sourceMetaInfo, targetTable);
                Assert.hasText(createTableDDL, "无法生成 CREATE TABLE DDL");

                // 执行 CREATE TABLE DDL
                DDLConfig ddlConfig = new DDLConfig();
                ddlConfig.setSql(createTableDDL);
                org.dbsyncer.common.model.Result result = connectorFactory.writerDDL(targetConnectorInstance, ddlConfig);

                if (StringUtil.isNotBlank(result.error)) {
                    logger.error("创建表失败: {}", result.error);
                    return RestResult.restFail("创建表失败: " + result.error, 500);
                }

                logger.info("成功创建目标表: {}", targetTable);
                return RestResult.restSuccess("创建表成功");

            } catch (UnsupportedOperationException e) {
                logger.error("连接器不支持自动生成 CREATE TABLE DDL: {}", targetConnector.getConfig().getConnectorType());
                return RestResult.restFail("该数据库类型不支持自动创建表: " + targetConnector.getConfig().getConnectorType(), 400);
            }

        } catch (IllegalArgumentException e) {
            logger.error("参数错误: {}", e.getMessage());
            return RestResult.restFail("参数错误: " + e.getMessage(), 400);
        } catch (Exception e) {
            logger.error("创建表异常: {}", e.getMessage(), e);
            return RestResult.restFail("创建表失败: " + e.getMessage(), 500);
        }
    }

}