package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;

import java.util.LinkedList;
import java.util.List;

/**
 * 解析add的属性（新增字段，只需要替换表名不需要替换sql）
 * exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
 *
 * @author life
 */
public class AddStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originalFieldMappings) {
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            boolean findColumn = false;
            List<String> columnSpecs = new LinkedList<>();
            // 对一before，after进行处理
            for (String spe : columnDataType.getColumnSpecs()) {
                spe = StringUtil.replace(spe, "`", "");
                if (findColumn) {
                    // 对before（after）字段进行映射
                    String finalSpe = spe;
                    FieldMapping fieldMapping = originalFieldMappings.stream().filter(x -> StringUtil.equals(x.getSource().getName(), finalSpe)).findFirst().get();
                    columnSpecs.add(fieldMapping.getTarget().getName());
                    findColumn = false;
                    continue;
                }

                if (StringUtil.equalsIgnoreCase(spe, "before") || StringUtil.equalsIgnoreCase(spe, "after")) {
                    findColumn = true;
                }
                columnSpecs.add(spe);
            }
            columnDataType.setColumnSpecs(columnSpecs);
            String columnName = StringUtil.replace(columnDataType.getColumnName(), "`", "");
            Field field = new Field(columnName, columnDataType.getColDataType().getDataType(), 0);//感觉不需要都行，只需要名称，后续可以自己刷新
            ddlConfig.getAddFields().add(field);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
    }
}
