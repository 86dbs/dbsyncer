package org.dbsyncer.connector.oracle.dialect;

import org.dbsyncer.sdk.connector.database.dialect.DefaultPreparedStatementSetterStrategy;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * oracle PreparedStatementStrategy
 *
 * <p>Oracle MERGE 语句中 USING (SELECT ? FROM DUAL) 的绑定变量默认按 VARCHAR2 处理，
 * 当 String 超过 4000 字节时会触发 ORA-01461，需对 CLOB/NCLOB 显式使用 setCharacterStream；
 * BLOB 则需要 setBinaryStream。</p>
 */
public final class OraclePreparedStatementStrategy extends DefaultPreparedStatementSetterStrategy {

    @Override
    public void setValue(PreparedStatement ps, int index, Object value, Integer jdbcType) throws SQLException {
        if (jdbcType == Types.BLOB && value != null) {
            ps.setBinaryStream(index, new ByteArrayInputStream((byte[]) value));
            return;
        }
        if ((jdbcType == Types.CLOB || jdbcType == Types.NCLOB || jdbcType == Types.LONGVARCHAR || jdbcType == Types.LONGNVARCHAR) && value != null) {
            String s = value.toString();
            ps.setCharacterStream(index, new StringReader(s), s.length());
            return;
        }
        super.setValue(ps, index, value, jdbcType);
    }
}
