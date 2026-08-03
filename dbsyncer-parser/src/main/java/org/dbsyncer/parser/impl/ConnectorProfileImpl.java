/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.constant.ConfigConstant;
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
}
