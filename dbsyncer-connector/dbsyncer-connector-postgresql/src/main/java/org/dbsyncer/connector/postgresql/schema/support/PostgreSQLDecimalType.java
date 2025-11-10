package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.DecimalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PostgreSQLDecimalType extends DecimalType {
    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("NUMERIC"));
    }

    @Override
    protected BigDecimal merge(Object val, Field field) {
        return throwUnsupportedException(val, field);
    }
    
}