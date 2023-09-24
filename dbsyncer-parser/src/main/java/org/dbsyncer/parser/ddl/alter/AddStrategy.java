package org.dbsyncer.parser.ddl.alter;

import java.util.LinkedList;
import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * 解析add的属性
 * exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
 * @author life
 */
public class AddStrategy implements ExpressionStrategy{

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig,
            List<FieldMapping> fieldMappingList) {
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            boolean findColumn = false;
            List<String> columnSpecs = new LinkedList<>();
            for (String spe:columnDataType.getColumnSpecs()) {//对一before，after进行处理
                spe = StringUtil.replace(spe,"`","");
                if (findColumn){
                    //对before（after）字段进行映射
                    String finalSpe = spe;
                    FieldMapping fieldMapping = fieldMappingList.stream().filter(x->x.getSource().getName().equals(
                            finalSpe)).findFirst().get();
                    columnSpecs.add(fieldMapping.getTarget().getName());
                    findColumn = false;
                    continue;
                }

                if (StringUtil.equalsIgnoreCase(spe,"before") || StringUtil.equalsIgnoreCase(spe,"after")){
                    findColumn =true;
                }
                columnSpecs.add(spe);
            }
            columnDataType.setColumnSpecs(columnSpecs);
            String columName = columnDataType.getColumnName();
            columName = StringUtil.replace(columName, "`", "");
            Field field = new Field(columName,columnDataType.getColDataType().getDataType(),0);//感觉不需要都行，只需要名称，后续可以自己刷新
            ddlConfig.getAddFields().add(field);
        }
    }
}
