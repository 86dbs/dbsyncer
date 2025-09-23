/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql;

import org.dbsyncer.connector.postgresql.cdc.DqlPostgreSQLListener;
import org.dbsyncer.connector.postgresql.validator.DqlPostgreSQLConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.plugin.ReaderContext;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.common.util.StringUtil;

/**
 * DQLSqlServer连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public final class DQLPostgreSQLConnector extends AbstractDQLConnector {

    /**
     * PostgreSQL引号字符
     */
    private static final String QUOTATION = "\"";

    private final DqlPostgreSQLConfigValidator configValidator = new DqlPostgreSQLConfigValidator();

    /**
     * 获取带引号的架构名
     */
    private String getSchemaWithQuotation(DatabaseConfig config) {
        StringBuilder schema = new StringBuilder();
        if (StringUtil.isNotBlank(config.getSchema())) {
            schema.append(QUOTATION).append(config.getSchema()).append(QUOTATION).append(".");
        }
        return schema.toString();
    }

    @Override
    public String getConnectorType() {
        return "DqlPostgreSQL";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlPostgreSQLListener();
        }
        return null;
    }


    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{pageSize, (pageIndex - 1) * pageSize};
    }

    @Override
    public String getQuotation() {
        return QUOTATION;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return context.getPageSize(); // 使用页面大小作为fetchSize
    }


}