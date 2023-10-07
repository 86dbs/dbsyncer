package org.dbsyncer.parser.ddl.alter;

import java.util.LinkedList;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.table.Index.ColumnParams;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;

import java.util.List;

/**
 * 解析drop

 *
 * @author life
 */
public class DropStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        if (expression.getColumnName() !=null){
           dropColumn(expression,ddlConfig,originalFieldMappings);
        }
        if (expression.getIndex() !=null){
            dropIndex(expression,originalFieldMappings);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
    }

    /**
     *  example: ALTER TABLE test_table DROP dis;
     * @param expression
     * @param ddlConfig
     * @param originalFieldMappings
     */
    private void dropColumn(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings){
        String columnName = StringUtil.replace(expression.getColumnName(), "`", "");
        Field field = new Field(columnName, null, 0);
        //需要把列替换成目标的列名
        originalFieldMappings.stream()
                .filter(x -> StringUtil.equals(x.getSource().getName(), columnName)).findFirst()
                .ifPresent(fieldMapping -> expression.setColumnName(fieldMapping.getTarget().getName()));
        //加入还是原名
        ddlConfig.getRemoveFields().add(field);
    }

    /**
     * 貌似不需要做什么，我们目前没有字段分索引，再考虑
     *  * example: ALTER TABLE test_table drop index name;
     * @param expression
     * @param originalFieldMappings
     */
    private void dropIndex(AlterExpression expression, List<FieldMapping> originalFieldMappings){
//        Index index = expression.getIndex();
//        String names= index.getName();
//        String[] nameList = StringUtil.split(names,".");
//        List<String> targetNameList = new LinkedList<>();
//        for (String name:nameList) {
//            FieldMapping fieldMapping = originalFieldMappings.stream()
//                    .filter(x -> StringUtil.equals(x.getSource().getName(),
//                            name)).findFirst().get();
//            targetNameList.add(fieldMapping.getTarget().getName());
//        }
//        index.setName(targetNameList);
    }
}