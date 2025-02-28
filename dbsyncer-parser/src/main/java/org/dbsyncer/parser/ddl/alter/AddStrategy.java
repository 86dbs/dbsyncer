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
 * 解析add的属性 exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
 *
 * @author life
 */
public class AddStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig) {
        if (expression.getColDataTypeList() != null) {
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                String columName = columnDataType.getColumnName();
                columName = StringUtil.replace(columName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                columName = StringUtil.replace(columName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
                ddlConfig.getAddedFieldNames().add(columName);
            }
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
    }
}