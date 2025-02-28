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
 * 字段属性变更
 * <code>
 * ALTER TABLE `test`.`test_user`
 * MODIFY COLUMN `name` varchar(203) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL AFTER `id`,
 * MODIFY COLUMN `remark` varchar(204) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL AFTER `name`
 * </code>
 *
 * @author life
 */
public class ModifyStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig) {
        if (expression.getColDataTypeList() != null) {
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                String columnName = StringUtil.replace(columnDataType.getColumnName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                columnName = StringUtil.replace(columnName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
                ddlConfig.getModifiedFieldNames().add(columnName);
            }
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_MODIFY);
    }
}