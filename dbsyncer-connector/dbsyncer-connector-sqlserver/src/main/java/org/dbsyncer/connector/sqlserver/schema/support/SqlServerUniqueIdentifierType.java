package org.dbsyncer.connector.sqlserver.schema.support;

import org.dbsyncer.sdk.schema.support.UUIDType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL Server UNIQUEIDENTIFIER类型支持
 * UNIQUEIDENTIFIER是SQL Server的GUID类型，标准化为UUID类型
 */
public final class SqlServerUniqueIdentifierType extends UUIDType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("UNIQUEIDENTIFIER"));
    }
}

