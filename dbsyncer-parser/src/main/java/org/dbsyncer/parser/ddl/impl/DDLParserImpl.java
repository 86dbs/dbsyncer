package org.dbsyncer.parser.ddl.impl;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.ddl.factory.ParserFactory;
import org.dbsyncer.parser.ddl.jsql.JsqParserFactory;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * alter情况
 * <ol>
 *     <li>只是修改字段的属性值</li>
 *     <li>修改字段的名称</li>
 * </ol>
 *
 * @author life
 * @version 1.0.0
 * @date 2023/9/19 22:38
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorFactory connectorFactory;

    @Override
    public DDLConfig parseDDlConfig(String sql, String targetConnectorType, String targetTableName,
            List<FieldMapping> fieldMappingList) {
        // 替换为目标库执行SQL
        DDLConfig ddlConfig = new DDLConfig();
        ParserFactory parserFactory = new JsqParserFactory(ddlConfig,fieldMappingList,connectorFactory,targetConnectorType,targetTableName);
        parserFactory.parser(sql);
        return ddlConfig;
    }

}