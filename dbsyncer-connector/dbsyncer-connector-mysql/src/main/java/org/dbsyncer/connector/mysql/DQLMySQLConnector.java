/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.cdc.DqlMySQLListener;
import org.dbsyncer.connector.mysql.validator.DqlMySQLConfigValidator;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.connector.database.AbstractDQLConnector;
import org.dbsyncer.sdk.constant.DatabaseConstant;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;
import org.dbsyncer.sdk.listener.DatabaseQuartzListener;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.sdk.plugin.ReaderContext;

/**
 * DQLMySQL连接器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public final class DQLMySQLConnector extends AbstractDQLConnector {

    /**
     * MySQL引号字符
     */
    private static final String QUOTATION = "`";

    private final DqlMySQLConfigValidator configValidator = new DqlMySQLConfigValidator();

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
        return "DqlMySQL";
    }

    @Override
    public ConfigValidator getConfigValidator() {
        return configValidator;
    }


    @Override
    public Object[] getPageArgs(ReaderContext context) {
        int pageSize = context.getPageSize();
        int pageIndex = context.getPageIndex();
        return new Object[]{(pageIndex - 1) * pageSize, pageSize};
    }

    @Override
    public Listener getListener(String listenerType) {
        if (ListenerTypeEnum.isTiming(listenerType)) {
            return new DatabaseQuartzListener();
        }

        if (ListenerTypeEnum.isLog(listenerType)) {
            return new DqlMySQLListener();
        }
        return null;
    }

    @Override
    public String generateUniqueCode() {
        return DatabaseConstant.DBS_UNIQUE_CODE;
    }

    @Override
    public Integer getStreamingFetchSize(ReaderContext context) {
        return Integer.MIN_VALUE; // MySQL流式处理特殊值
    }
}