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
 * 解析change属性
 * <code>
 *     ALTER TABLE `test`.`test_user`
 * CHANGE COLUMN `name` `name2` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL AFTER `id`,
 * CHANGE COLUMN `remark` `remark2` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL AFTER `name2`
 * </code>
 *
 * @author life
 */
public class ChangeStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig) {
        if (expression.getColDataTypeList() != null) {
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                String oldColumnName = StringUtil.replace(expression.getColumnOldName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                oldColumnName = StringUtil.replace(oldColumnName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);

                String changedColumnName = StringUtil.replace(columnDataType.getColumnName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                changedColumnName = StringUtil.replace(changedColumnName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
                ddlConfig.getChangedFieldNames().put(oldColumnName, changedColumnName);
            }
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_CHANGE);
    }
}