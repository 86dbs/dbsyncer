/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.oracle.cdc.OracleListener;
import org.dbsyncer.connector.oracle.schema.OracleClobValueMapper;
import org.dbsyncer.connector.oracle.schema.OracleOtherValueMapper;
import org.dbsyncer.connector.oracle.validator.OracleConfigValidator;
import org.dbsyncer.sdk.config.CommandConfig;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.connector.database.sql.context.SqlBuildContext;
import org.dbsyncer.sdk.connector.database.sql.impl.OracleTemplate;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-12 21:14
 */
public class OracleConnector extends AbstractDatabaseConnector {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final ConfigValidator<?> configValidator = new OracleConfigValidator();
    public final SqlTemplate sqlTemplate = new OracleTemplate();

    public OracleConnector() {
        VALUE_MAPPERS.put(Types.OTHER, new OracleOtherValueMapper());
        VALUE_MAPPERS.put(Types.CLOB, new OracleClobValueMapper());
    }

    @Override
    public String getConnectorType() {
        return "Oracle";
    }

    @Override
    public Map<String, String> getPosition(org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance connectorInstance) {
        // 查询当前SCN号
        String currentSCN = connectorInstance.execute(databaseTemplate ->
                databaseTemplate.queryForObject("SELECT CURRENT_SCN FROM V$DATABASE", String.class));

        if (currentSCN != null) {
            // 创建与snapshot中存储格式一致的position信息
            Map<String, String> position = new HashMap<>();
            position.put("position", currentSCN);
            logger.debug("成功获取Oracle当前SCN: {}", currentSCN);
            return position;
        }
        return null;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new OracleListener();
        }
        return null;
    }

    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageIndex * pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public Object[] getPageCursorArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        Object[] cursors = context.getCursors();
        if (null == cursors) {
            return new Object[]{pageSize, 0};
        }
        int cursorsLen = cursors.length;
        Object[] newCursors = new Object[cursorsLen + 2];
        System.arraycopy(cursors, 0, newCursors, 0, cursorsLen);
        newCursors[cursorsLen] = pageSize;
        newCursors[cursorsLen + 1] = 0;
        return newCursors;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }

    @Override
    public String getValidationQuery() {
        return "select 1 from dual";
    }




}