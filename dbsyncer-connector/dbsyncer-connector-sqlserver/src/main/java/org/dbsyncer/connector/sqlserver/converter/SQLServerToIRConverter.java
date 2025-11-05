package org.dbsyncer.connector.sqlserver.converter;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.sqlserver.schema.SqlServerSchemaResolver;
import org.dbsyncer.sdk.parser.ddl.converter.AbstractSourceToIRConverter;

/**
 * SQL Server到中间表示转换器
 */
public class SQLServerToIRConverter extends AbstractSourceToIRConverter {

    public SQLServerToIRConverter() {
        // 初始化SchemaResolver
        this.schemaResolver = new SqlServerSchemaResolver();
    }

    @Override
    protected String removeIdentifier(String name) {
        if (name != null) {
            return StringUtil.replace(name, "[", "").replace("]", "");
        }
        return name;
    }
}