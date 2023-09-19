package org.dbsyncer.parser.ddl.impl;

import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.ddl.DDLParser;
import org.springframework.stereotype.Component;

@Component
public class DDLParserImpl implements DDLParser {

    @Override
    public DDLConfig parseDDlConfig(String sql, String targetTableName) {
        DDLConfig ddlConfig = new DDLConfig();
        // TODO life 替换为目标库执行SQL
//        try {
//            Alter alter = (Alter) CCJSqlParserUtil.parse(sql);
//        } catch (JSQLParserException e) {
//            logger.error(e.getMessage(), e);
//        }
        ddlConfig.setSql(sql);
        return ddlConfig;
    }
}