/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;

/**
 * 解析drop
 *
 * @author life
 */
public class DropStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig) {
        if (expression.getColumnName() != null) {
            String columnName = StringUtil.replace(expression.getColumnName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            columnName = StringUtil.replace(columnName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
            ddlConfig.getDroppedFieldNames().add(columnName);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
    }
}