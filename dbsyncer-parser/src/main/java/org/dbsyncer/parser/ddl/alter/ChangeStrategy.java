package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;

import java.util.List;

/**
 * 解析change属性
 * exampleSql: ALTER TABLE test_table CHANGE duan1  duan2 INT(10)
 *
 * @author life
 */
public class ChangeStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        String oldColumnName = StringUtil.replace(expression.getColumnOldName(), "`", "");
        ddlConfig.setSourceColumnName(oldColumnName);
        FieldMapping fieldMapping = originalFieldMappings.stream().filter(x -> StringUtil.equals(x.getSource().getName(), oldColumnName)).findFirst().orElse(null);
        if (fieldMapping != null) {
            expression.setColumnOldName(fieldMapping.getTarget().getName());
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                ddlConfig.setChangedColumnName(columnDataType.getColumnName());
            }
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_CHANGE);
    }
}
