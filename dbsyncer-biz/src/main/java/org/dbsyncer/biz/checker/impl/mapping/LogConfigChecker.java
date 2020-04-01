/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.connector.config.ConnectorConfig;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 日志配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class LogConfigChecker implements MappingConfigChecker {

    @Autowired
    private Manager manager;

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        // String label = params.get("incrementStrategyTableLabel");
        // TODO 仅支持 DQL_Mysql/DQL_Oracle
        String connectorId = mapping.getSourceConnectorId();
        Connector connector = manager.getConnector(connectorId);
        ConnectorConfig config = connector.getConfig();
        String type = config.getConnectorType();
        if(StringUtils.equals(ConnectorEnum.DQL_MYSQL.getType(),type)){

            return;
        }
//        if(StringUtils.equals(ConnectorEnum.DQL_ORACLE.getType(),type)){
//
//            return;
//        }
    }

}