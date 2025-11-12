package org.dbsyncer.connector.sqlserver.converter;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
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

    @Override
    protected AlterOperation mapAlterOperation(AlterExpression expr) {
        // SQL Server 使用 ALTER COLUMN 语法，语义上等同于 MODIFY COLUMN
        // 将其映射到 MODIFY 操作类型
        return AlterOperation.MODIFY;
    }
}