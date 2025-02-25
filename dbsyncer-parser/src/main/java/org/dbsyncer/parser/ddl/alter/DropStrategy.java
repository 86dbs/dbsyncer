package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;

import java.util.List;

/**
 * 解析drop
 *
 * @author life
 */
public class DropStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        if (expression.getColumnName() != null) {
            dropColumn(expression, ddlConfig);
        }
        if (expression.getIndex() != null) {
            dropIndex(expression, originalFieldMappings);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
    }

    /**
     * example: ALTER TABLE test_table DROP dis;
     *
     * @param expression
     * @param ddlConfig
     */
    private void dropColumn(AlterExpression expression, DDLConfig ddlConfig) {
        String columnName = StringUtil.replace(expression.getColumnName(), StringUtil.BACK_QUOTE, StringUtil.EMPTY);
        columnName = StringUtil.replace(columnName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
        ddlConfig.getRemoveFieldNames().add(columnName);
    }

    /**
     * 貌似不需要做什么，我们目前没有字段分索引，再考虑
     * example: ALTER TABLE test_table drop index name;
     *
     * @param expression
     * @param originalFieldMappings
     */
    private void dropIndex(AlterExpression expression, List<FieldMapping> originalFieldMappings) {
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