/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link ConnectorProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 */
@Component
public class ConnectorProfileImpl implements ConnectorProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public Connector parseConnector(String json) {
        Map conn = JsonUtil.parseMap(json);
        Map config = (Map) conn.remove("config");
        Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);
        Assert.notNull(connector, "Connector can not be null.");
        if (config != null) {
            String connectorType = (String) config.get("connectorType");
            ConnectorService connectorService = connectorFactory.getConnectorService(connectorType);
            Class<ConnectorConfig> configClass = connectorService.getConfigClass();
            connector.setConfig(JsonUtil.jsonToObj(config.toString(), configClass));
        }
        return connector;
    }

    @Override
    public Connector getConnector(String connectorId) {
        if (StringUtil.isBlank(connectorId)) {
            return null;
        }
        Query query = new Query();
        query.setType(StorageEnum.CONNECTOR);
        query.setPageNum(1);
        query.setPageSize(1);
        query.addFilter(ConfigConstant.CONFIG_MODEL_ID, connectorId);
        Paging paging = storageService.query(query);
        List<Map> data = paging == null ? null : (List<Map>) paging.getData();
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        Object json = data.get(0).get(ConfigConstant.CONFIG_MODEL_JSON);
        return json == null ? null : parseConnector(String.valueOf(json));
    }

    @Override
    public List<Connector> getConnectorAll() {
        List<Connector> result = new ArrayList<>();
        Query query = new Query();
        query.setType(StorageEnum.CONNECTOR);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
                if (json != null) {
                    Connector connector = parseConnector(String.valueOf(json));
                    if (connector != null) {
                        result.add(connector);
                    }
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        return result;
    }

    @Override
    public Paging<Connector> queryConnectors(int pageNum, int pageSize, String searchKey) {
        int safePageNum = pageNum > 0 ? pageNum : 1;
        int safePageSize = pageSize > 0 ? pageSize : ConfigConstant.PAGE_SIZE;
        Query query = new Query(safePageNum, safePageSize);
        query.setType(StorageEnum.CONNECTOR);
        if (StringUtil.isNotBlank(searchKey)) {
            query.addFilter(ConfigConstant.CONFIG_MODEL_NAME, searchKey, false);
        }
        query.addOrderBy(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, SortEnum.DESC);
        Paging paging = storageService.query(query);
        Paging<Connector> result = new Paging<>(safePageNum, safePageSize);
        if (paging == null) {
            return result;
        }
        result.setTotal(paging.getTotal());
        if (CollectionUtils.isEmpty(paging.getData())) {
            return result;
        }
        List<Connector> rows = new ArrayList<>(paging.getData().size());
        for (Object item : paging.getData()) {
            if (!(item instanceof Map)) {
                continue;
            }
            Object json = ((Map) item).get(ConfigConstant.CONFIG_MODEL_JSON);
            if (json == null) {
                continue;
            }
            Connector connector = parseConnector(String.valueOf(json));
            if (connector != null) {
                rows.add(connector);
            }
        }
        result.setData(rows);
        return result;
    }

    @Override
    public String addConnector(Connector connector) {
        return operationTemplate.execute(new OperationConfig(connector, CommandEnum.OPR_ADD));
    }

    @Override
    public void addConnectorBatch(List<Connector> connectors) {
        if (CollectionUtils.isEmpty(connectors)) {
            return;
        }
        TaskSplitUtil.split(connectors, ConfigConstant.PAGE_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    @Override
    public String updateConnector(Connector connector) {
        return operationTemplate.execute(new OperationConfig(connector, CommandEnum.OPR_EDIT));
    }

    @Override
    public void removeConnector(String id) {
        operationTemplate.remove(new OperationConfig(id));
    }

    @Override
    public int countConnectors() {
        return operationTemplate.count(StorageEnum.CONNECTOR, null);
    }

    @Override
    public void importConnectorsFromJson(String json) {
        if (StringUtil.isBlank(json)) {
            return;
        }
        List list = JsonUtil.parseList(json);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        List<Connector> connectors = new ArrayList<>(list.size());
        for (Object item : list) {
            Connector connector = parseConnector(JsonUtil.objToJson(item));
            if (connector != null) {
                connectors.add(connector);
            }
        }
        TaskSplitUtil.split(connectors, PackageFormatConfig.IMPORT_BATCH_SIZE, this::addConnectorBatch);
    }
}
