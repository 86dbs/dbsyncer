/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database.sql.impl;

import org.dbsyncer.sdk.connector.database.sql.SqlTemplate;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.SchemaResolver;

/**
 * 抽象SQL模板实现
 * 提供SchemaResolver支持的基础实现
 */
public abstract class AbstractSqlTemplate implements SqlTemplate {

    protected SchemaResolver schemaResolver;

    public AbstractSqlTemplate(SchemaResolver schemaResolver) {
        this.schemaResolver = schemaResolver;
    }

    public String convertToDatabaseType(Field column) {
        return schemaResolver.fromStandardType(column).getTypeName();
    }
}