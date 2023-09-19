package org.dbsyncer.parser.ddl.impl;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public DDLConfig parseDDlConfig(String sql, String targetTableName,
            List<FieldMapping> fieldMappingList) {
        DDLConfig ddlConfig = new DDLConfig();
        // TODO life 替换为目标库执行SQL
        try {
            Alter alter = (Alter) CCJSqlParserUtil.parse(sql);
            for (AlterExpression alterExpression:alter.getAlterExpressions()) {
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
            }
            sql = alter.toString();
            logger.info("目标sql为"+sql);
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
        ddlConfig.setSql(sql);
        return ddlConfig;
    }
}