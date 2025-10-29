/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;

/**
 * 异构数据库DDL转换器
 */
public interface HeterogeneousDDLConverter {

    /**
     * 将源数据库DDL转换为中间表示
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param alter               源DDL语句解析对象
     * @return 中间表示
     */
    DDLIntermediateRepresentation parseToIR(String sourceConnectorType, Alter alter);

    /**
     * 将中间表示转换为目标数据库DDL
     *
     * @param targetConnectorType 目标数据库连接器类型
     * @param ir                  中间表示
     * @return 目标数据库DDL语句
     */
    String generateFromIR(String targetConnectorType, DDLIntermediateRepresentation ir);

    /**
     * 检查是否支持指定数据库类型的转换
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param targetConnectorType 目标数据库连接器类型
     * @return 是否支持转换
     */
    boolean supports(String sourceConnectorType, String targetConnectorType);
}