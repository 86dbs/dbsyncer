package org.dbsyncer.parser.ddl.impl;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * alter情况：<br>
 * 情况1:只是修改字段的属性值<br>
 * 情况2:修改字段的名称
 *
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public DDLConfig parseDDlConfig(String sql, String targetTableName,
            List<FieldMapping> fieldMappingList) {
        DDLConfig ddlConfig = new DDLConfig();
        // TODO life 替换为目标库执行SQL
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof  Alter){ //alter语句
                Alter alter = (Alter) statement;
                //先替换成目标表名
                alter.setTable(new Table(targetTableName));
                for (AlterExpression alterExpression:alter.getAlterExpressions()) {
                    AlterOperation operation =alterExpression.getOperation();
                    if (operation == AlterOperation.MODIFY){//修改列的属性
                        for (AlterExpression.ColumnDataType columnDataType:alterExpression.getColDataTypeList()) {
                            String columName = columnDataType.getColumnName();
                            columName = columName.replaceAll("`","");
                            for (FieldMapping fieldMapping :fieldMappingList) {
                                if (fieldMapping.getSource().getName().equals(columName)){
                                    columnDataType.setColumnName(fieldMapping.getTarget().getName());
                                    break;
                                }
                            }
                        }
                        sql = alter.toString();
                    }else if (operation == AlterOperation.CHANGE){//修改列名

                    }

                }
            }
            logger.info("目标sql为"+sql);
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
        ddlConfig.setSql(sql);
        return ddlConfig;
    }

}