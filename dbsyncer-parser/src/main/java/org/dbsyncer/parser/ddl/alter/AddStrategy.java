package org.dbsyncer.parser.ddl.alter;

import java.util.LinkedList;
import java.util.List;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.table.Index.ColumnParams;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * 解析add的属性 exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
 *
 * @author life
 */
public class AddStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig,
            List<FieldMapping> origionFiledMapping) {
        if (expression.getColDataTypeList() != null) {
            parseAddColumn(expression, ddlConfig, origionFiledMapping);
        }
        if (expression.getIndex() != null) {
            parseAddIndex(expression, origionFiledMapping);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
    }

    //解析增加列
    //exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
    private void parseAddColumn(AlterExpression expression, DDLConfig ddlConfig,
            List<FieldMapping> origionFiledMapping) {
        //如果是增加列
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            boolean findColumn = false;
            List<String> columnSpecs = new LinkedList<>();
            for (String spe : columnDataType.getColumnSpecs()) {//对一before，after进行处理
                spe = StringUtil.replace(spe, "`", "");
                if (findColumn) {
                    //对before（after）字段进行映射
                    String finalSpe = spe;
                    FieldMapping fieldMapping = origionFiledMapping.stream()
                            .filter(x -> StringUtil.equals(x.getSource().getName(), finalSpe))
                            .findFirst().get();
                    columnSpecs.add(fieldMapping.getTarget().getName());
                    findColumn = false;
                    continue;
                }

                if (StringUtil.equalsIgnoreCase(spe, "before") || StringUtil.equalsIgnoreCase(spe,
                        "after")) {
                    findColumn = true;
                }
                columnSpecs.add(spe);
            }
            columnDataType.setColumnSpecs(columnSpecs);
            String columName = columnDataType.getColumnName();
            columName = StringUtil.replace(columName, "`", "");
            Field field = new Field(columName, columnDataType.getColDataType().getDataType(),
                    0);//感觉不需要都行，只需要名称，后续可以自己刷新
            ddlConfig.getAddFields().add(field);
        }

    }

    /**
     * 新增索引 exampleSql: ALTER TABLE test_table add index name (tmp);
     *
     * @param expression
     * @param fieldMappingList
     */
    private void parseAddIndex(AlterExpression expression,
            List<FieldMapping> fieldMappingList) {
        Index index = expression.getIndex();
        List<ColumnParams> columnNames = index.getColumns();
        List<ColumnParams> targetNames = new LinkedList<>();
        for (ColumnParams columnParams : columnNames) {
            FieldMapping fieldMapping = fieldMappingList.stream()
                    .filter(x -> StringUtil.equals(x.getSource().getName(),
                            columnParams.getColumnName())).findFirst().get();
            ColumnParams target = new ColumnParams(fieldMapping.getTarget().getName(),
                    columnParams.getParams());
            targetNames.add(target);
        }
        index.setColumns(targetNames);
    }
}
