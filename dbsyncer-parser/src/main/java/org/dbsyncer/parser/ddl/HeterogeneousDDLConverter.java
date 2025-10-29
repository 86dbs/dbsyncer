/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.statement.alter.Alter;

/**
 * 异构数据库DDL转换器
 */
public interface HeterogeneousDDLConverter {

    /**
     * 直接将源数据库DDL转换为目标数据库DDL
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param targetConnectorType 目标数据库连接器类型
     * @param alter               源DDL语句解析对象
     * @return 目标数据库DDL语句
     */
    String convert(String sourceConnectorType, String targetConnectorType, Alter alter);

    /**
     * 检查是否支持指定数据库类型的转换
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param targetConnectorType 目标数据库连接器类型
     * @return 是否支持转换
     */
    boolean supports(String sourceConnectorType, String targetConnectorType);
}