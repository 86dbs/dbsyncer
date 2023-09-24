package org.dbsyncer.parser.ddl.jsql;

import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.parser.ddl.alter.AlterStrategy;
import org.dbsyncer.parser.ddl.factory.ParserFactory;
import org.dbsyncer.parser.model.FieldMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author life
 */
public class JsqParserFactory extends ParserFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    public JsqParserFactory(DDLConfig ddlConfig, List<FieldMapping> fieldMappingList,
            ConnectorFactory connectorFactory, String targetConnectorType, String targetTableName) {
        super(ddlConfig, fieldMappingList, connectorFactory, targetConnectorType, targetTableName);
    }

    @Override
    public void parser(String sql, String targetName) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                AlterStrategy alterStrategy = new AlterStrategy((Alter) statement,ddlConfig,fieldMappingList,targetName);
                alterStrategy.parser();
            }
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
