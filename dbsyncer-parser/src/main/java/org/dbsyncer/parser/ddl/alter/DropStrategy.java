package org.dbsyncer.parser.ddl.alter;

import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * 解析drop
 * example: ALTER TABLE test_table DROP dis;
 * @author life
 */
public class DropStrategy implements ExpressionStrategy{

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig,
            List<FieldMapping> fieldMappingList) {
        String columName = expression.getColumnName();
        columName = StringUtil.replace(columName, "`", "");
        Field field = new Field(columName,null,0);
        //需要把列替换成目标的列名
        String finalColumName = columName;
        fieldMappingList.stream().filter(x -> x.getSource().getName().equals(
                finalColumName)).findFirst().ifPresent(
                fieldMapping -> expression.setColumnName(fieldMapping.getTarget().getName()));
        //加入还是原名
        ddlConfig.getRemoveFields().add(field);
    }
}
