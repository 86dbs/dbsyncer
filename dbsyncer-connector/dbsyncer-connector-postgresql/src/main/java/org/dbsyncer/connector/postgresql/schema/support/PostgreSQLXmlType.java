package org.dbsyncer.connector.postgresql.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.XmlType;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * PostgreSQL XML类型支持
 */
public final class PostgreSQLXmlType extends XmlType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("XML"));
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        if (val instanceof PGobject) {
            PGobject pgObject = (PGobject) val;
            return pgObject.getValue();
        }
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof String) {
            try {
                PGobject xml = new PGobject();
                xml.setType("xml");
                xml.setValue((String) val);
                return xml;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return super.convert(val, field);
    }
}