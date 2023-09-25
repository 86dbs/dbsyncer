package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;

import java.util.List;

/**
 * 解析modify的属性
 * exampleSql: ALTER TABLE `test`.`test_table` MODIFY COLUMN `test` varchar(251) NULL DEFAULT NULL
 * alter modify parser
 *
 * @author life
 */
public class ModifyStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        //先查找到当前的表和目标的表对应的字段
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            String columnName = StringUtil.replace(columnDataType.getColumnName(), "`", "");
            for (FieldMapping fieldMapping : originalFieldMappings) {
                if (StringUtil.equals(fieldMapping.getSource().getName(), columnName)) {
                    //TODO life 找到目标的表名，先是alter进行属性替换，然后config记录新的
                    columnDataType.setColumnName(fieldMapping.getTarget().getName());
                    //因为只是修改属性，所以表名称没有变化
                    ddlConfig.setSourceColumnName(fieldMapping.getSource().getName());
                    ddlConfig.setChangedColumnName(fieldMapping.getSource().getName());
                }
            }
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_MODIFY);
    }
}
