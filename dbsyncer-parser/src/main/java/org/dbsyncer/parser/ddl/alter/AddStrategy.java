package org.dbsyncer.parser.ddl.alter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.table.Index.ColumnParams;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.enums.DDLOperationEnum;

import java.util.LinkedList;
import java.util.List;

/**
 * 解析add的属性 exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
 *
 * @author life
 */
public class AddStrategy implements AlterStrategy {

    @Override
    public void parse(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originFiledMapping) {
        if (expression.getColDataTypeList() != null) {
            parseAddColumn(expression, ddlConfig, originFiledMapping);
        }
        if (expression.getIndex() != null) {
            parseAddIndex(expression, originFiledMapping);
        }
        ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
    }

    //解析增加列
    //exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
    private void parseAddColumn(AlterExpression expression, DDLConfig ddlConfig, List<FieldMapping> originFiledMapping) {
        //如果是增加列
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            boolean findColumn = false;
            List<String> columnSpecs = new LinkedList<>();
            for (String spe : columnDataType.getColumnSpecs()) {//对一before，after进行处理
                spe = StringUtil.replace(spe, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
                spe = StringUtil.replace(spe, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
                if (findColumn) {
                    //对before（after）字段进行映射
                    String finalSpe = spe;
                    FieldMapping fieldMapping = originFiledMapping.stream().filter(x -> StringUtil.equals(x.getSource().getName(), finalSpe)).findFirst().get();
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
            String columName = columnDataType.getColumnName();
            columName = StringUtil.replace(columName, StringUtil.BACK_QUOTE, StringUtil.EMPTY);
            columName = StringUtil.replace(columName, StringUtil.DOUBLE_QUOTATION, StringUtil.EMPTY);
            ddlConfig.getAddFieldNames().add(columName);
        }

    }

    /**
     * 新增索引 exampleSql: ALTER TABLE test_table add index name (tmp);
     *
     * @param expression
     * @param originFiledMapping
     */
    private void parseAddIndex(AlterExpression expression, List<FieldMapping> originFiledMapping) {
        Index index = expression.getIndex();
        List<ColumnParams> columnNames = index.getColumns();
        List<ColumnParams> targetNames = new LinkedList<>();
        for (ColumnParams columnParams : columnNames) {
            FieldMapping fieldMapping = originFiledMapping.stream().filter(x -> StringUtil.equals(x.getSource().getName(), columnParams.getColumnName())).findFirst().get();
            ColumnParams target = new ColumnParams(fieldMapping.getTarget().getName(), columnParams.getParams());
            targetNames.add(target);
        }
        index.setColumns(targetNames);
    }
}
