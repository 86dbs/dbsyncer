package org.dbsyncer.parser.ddl.alter;

import java.util.LinkedList;
import java.util.List;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.enums.DDLOperationEnum;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.parser.ddl.strategy.JsqlParserStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author life
 */
public class AlterStrategy implements JsqlParserStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    Alter alter;

    DDLConfig ddlConfig;

    List<FieldMapping> fieldMappingList;

    String targetName;

    public AlterStrategy(Alter alter, DDLConfig ddlConfig, List<FieldMapping> fieldMappingList,
            String targetName) {
        this.alter = alter;
        this.ddlConfig = ddlConfig;
        this.fieldMappingList = fieldMappingList;
        this.targetName = targetName;
    }

    @Override
    public void parser() {
        // 替换成目标表名
        alter.getTable().setName(targetName);
        for (AlterExpression expression : alter.getAlterExpressions()) {
            AlterOperation alterOperation = expression.getOperation();
            if (alterOperation == AlterOperation.MODIFY) {//修改属性
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_MODIFY);
                parseModify(expression);
            } else if (AlterOperation.ADD == alterOperation) {//新增字段，只需要替换表名不需要替换sql
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
                parseAdd(expression);
            }else if (AlterOperation.CHANGE == alterOperation){
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_CHANGE);
                parseChange(expression);
            }else if (AlterOperation.DROP == alterOperation){
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
                parseDrop(expression);
            }
            ddlConfig.setSql(alter.toString());
        }

    }

    //解析drop
    //example: ALTER TABLE test_table DROP dis;
    private void parseDrop(AlterExpression expression) {
        String columName = expression.getColumnName();
        columName = StringUtil.replace(columName, "`", "");
        Field field = new Field(columName,null,0);
        //需要把列替换成目标的列名
        String finalColumName = columName;
        FieldMapping fieldMapping =  fieldMappingList.stream().filter(x->x.getSource().getName().equals(
                finalColumName)).findFirst().orElse(null);
        if (fieldMapping !=null){
            expression.setColumnName(fieldMapping.getTarget().getName());
        }
        //加入还是原名
        ddlConfig.getRemoveFields().add(field);
    }

    //解析change属性
    //exampleSql: ALTER TABLE test_table CHANGE duan1  duan2 INT(10)
    private void parseChange(AlterExpression expression) {
        String oldColumnName = expression.getColumnOldName();
        oldColumnName =StringUtil.replace(oldColumnName,"`","");
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

    //解析add的属性
    //exampleSql: ALTER TABLE cost ADD duan INT after(before) `tmp`;
    private void parseAdd(AlterExpression expression) {
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

    //解析modify的属性
    //exampleSql: ALTER TABLE `test`.`test_table` MODIFY COLUMN `test` varchar(251) NULL DEFAULT NULL
    private void parseModify(AlterExpression expression){
        //先查找到当前的表和目标的表对应的字段
        for (AlterExpression.ColumnDataType columnDataType : expression.getColDataTypeList()) {
            String columName = columnDataType.getColumnName();
            columName = StringUtil.replace(columName, "`", "");
            for (FieldMapping fieldMapping : fieldMappingList) {
                if (StringUtil.equals(fieldMapping.getSource().getName(), columName)) {
                    //TODO life 找到目标的表名，先是alter进行属性替换，然后config记录新的
                    columnDataType.setColumnName(
                            fieldMapping.getTarget().getName());//alter语法树进行替换
                    //因为只是修改属性，所以表名称没有变化
                    ddlConfig.setSourceColumnName(fieldMapping.getSource().getName());
                    ddlConfig.setChangedColumnName(fieldMapping.getSource().getName());
                }
            }
        }
    }
}
