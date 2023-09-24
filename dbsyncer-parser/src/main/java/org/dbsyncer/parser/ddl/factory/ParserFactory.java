package org.dbsyncer.parser.ddl.factory;

import java.util.List;
import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.config.DDLConfig;
import org.dbsyncer.connector.database.Database;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.model.FieldMapping;

/**
 * @author life
 */
public abstract class ParserFactory {

    public DDLConfig ddlConfig;

    public List<FieldMapping> fieldMappingList;

    ConnectorFactory connectorFactory;

    String targetConnectorType;

    String targetTableName;

    public ParserFactory(DDLConfig ddlConfig, List<FieldMapping> fieldMappingList,
            ConnectorFactory connectorFactory, String targetConnectorType, String targetTableName) {
        this.ddlConfig = ddlConfig;
        this.fieldMappingList = fieldMappingList;
        this.connectorFactory = connectorFactory;
        this.targetConnectorType = targetConnectorType;
        this.targetTableName = targetTableName;
    }

    public void parser(String sql){
        Connector connector = connectorFactory.getConnector(targetConnectorType);
        // 暂支持关系型数据库解析
        if (!(connector instanceof Database)) {
            throw new ParserException("暂支持关系型数据库解析");
        }
        Database database = (Database) connector;
        String quotation = database.buildSqlWithQuotation();
        String tableName = new StringBuilder(quotation).append(targetTableName).append(quotation).toString();
        parser(sql,tableName);
    }


    public abstract void parser(String sql,String targetName);

}
