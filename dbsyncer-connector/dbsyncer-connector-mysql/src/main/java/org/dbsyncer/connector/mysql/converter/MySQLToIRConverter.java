package org.dbsyncer.connector.mysql.converter;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mysql.schema.MySQLSchemaResolver;
import org.dbsyncer.sdk.parser.ddl.converter.AbstractSourceToIRConverter;

/**
 * MySQL到中间表示转换器
 */
public class MySQLToIRConverter extends AbstractSourceToIRConverter {

    public MySQLToIRConverter() {
        // 初始化SchemaResolver
        this.schemaResolver = new MySQLSchemaResolver();
    }

    @Override
    protected String removeIdentifier(String name) {
        if (name != null) {
            return StringUtil.replace(name, "`", "");
        }
        return name;
    }
}