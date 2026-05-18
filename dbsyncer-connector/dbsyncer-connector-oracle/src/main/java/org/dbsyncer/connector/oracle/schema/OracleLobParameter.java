package org.dbsyncer.connector.oracle.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.BindParameter;

import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/**
 * Oracle CLOB/NCLOB/LONG 写入绑定（须使用连接创建 LOB，MERGE 场景不支持 SerialClob）。
 */
public final class OracleLobParameter implements BindParameter {

    private final String value;
    private final String typeName;

    public OracleLobParameter(String value, Field field) {
        this.value = value;
        this.typeName = field == null ? null : field.getTypeName();
    }

    @Override
    public void setValue(PreparedStatement ps, int paramIndex, Connection connection) throws SQLException {
        if (value == null) {
            ps.setNull(paramIndex, sqlTypeForNull());
            return;
        }
        String type = normalizeTypeName();
        if ("NCLOB".equals(type)) {
            NClob nclob = connection.createNClob();
            nclob.setString(1, value);
            ps.setNClob(paramIndex, nclob);
            return;
        }
        if ("LONG".equals(type)) {
            ps.setCharacterStream(paramIndex, new StringReader(value), value.length());
            return;
        }
        Clob clob = connection.createClob();
        clob.setString(1, value);
        ps.setClob(paramIndex, clob);
    }

    private int sqlTypeForNull() {
        String type = normalizeTypeName();
        if ("NCLOB".equals(type)) {
            return Types.NCLOB;
        }
        if ("LONG".equals(type)) {
            return Types.LONGVARCHAR;
        }
        return Types.CLOB;
    }

    private String normalizeTypeName() {
        if (StringUtil.isBlank(typeName)) {
            return "CLOB";
        }
        return typeName.trim().toUpperCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return value == null ? StringUtil.EMPTY : value;
    }
}
