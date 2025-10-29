/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.spi.ConnectorService;

/**
 * 异构数据库DDL转换器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 20:00
 */
public interface HeterogeneousDDLConverter {

    /**
     * 转换异构数据库DDL语句
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param targetConnectorType 目标数据库连接器类型
     * @param alter               源DDL语句解析对象
     * @param ddlConfig           DDL配置对象
     * @return 转换后的DDL语句
     */
    String convert(String sourceConnectorType, String targetConnectorType, Alter alter, DDLConfig ddlConfig);
    
    /**
     * 检查是否支持指定数据库类型的转换
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param targetConnectorType 目标数据库连接器类型
     * @return 是否支持转换
     */
    boolean supports(String sourceConnectorType, String targetConnectorType);
}