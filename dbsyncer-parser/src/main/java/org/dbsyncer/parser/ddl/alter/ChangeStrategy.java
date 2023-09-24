package org.dbsyncer.parser.ddl.alter;

import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * 解析change属性
 * exampleSql: ALTER TABLE test_table CHANGE duan1  duan2 INT(10)
 * @author life
 */
public class ChangeStrategy implements ExpressionStrategy{


    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig,
            List<FieldMapping> fieldMappingList) {
        String oldColumnName = expression.getColumnOldName();
        oldColumnName = StringUtil.replace(oldColumnName,"`","");
        ddlConfig.setSourceColumnName(oldColumnName);
        String finalOldColumnName = oldColumnName;
        FieldMapping fieldMapping = fieldMappingList.stream().filter(x->x.getSource().getName().equals(
                finalOldColumnName)).findFirst().orElse(null);
        if (fieldMapping != null) {
            expression.setColumnOldName(fieldMapping.getTarget().getName());
            for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
                ddlConfig.setChangedColumnName(columnDataType.getColumnName());
            }
        }

    }
}
