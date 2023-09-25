package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;

import java.util.List;

/**
 * 解析drop
 * example: ALTER TABLE test_table DROP dis;
 *
 * @author life
 */
public class DropStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        String columnName = StringUtil.replace(expression.getColumnName(), "`", "");
        Field field = new Field(columnName, null, 0);
        //需要把列替换成目标的列名
        originalFieldMappings.stream().filter(x -> StringUtil.equals(x.getSource().getName(), columnName)).findFirst().ifPresent(
                fieldMapping -> expression.setColumnName(fieldMapping.getTarget().getName()));
        //加入还是原名
        ddlConfig.getRemoveFields().add(field);
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
    }
}