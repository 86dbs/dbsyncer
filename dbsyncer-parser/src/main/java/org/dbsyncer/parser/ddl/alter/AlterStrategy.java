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
            ExpressionStrategy expressionStrategy = null;
            if (alterOperation == AlterOperation.MODIFY) {//修改属性
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_MODIFY);
                expressionStrategy = new ModifyStrategy();
            } else if (AlterOperation.ADD == alterOperation) {//新增字段，只需要替换表名不需要替换sql
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_ADD);
                expressionStrategy =new AddStrategy();
            }else if (AlterOperation.CHANGE == alterOperation){
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_CHANGE);
                expressionStrategy = new ChangeStrategy();
            }else if (AlterOperation.DROP == alterOperation){
                ddlConfig.setDdlOperationEnum(DDLOperationEnum.ALTER_DROP);
                expressionStrategy = new DropStrategy();
            }
            expressionStrategy.parse(expression,ddlConfig,fieldMappingList);
            ddlConfig.setSql(alter.toString());
        }

    }
}
