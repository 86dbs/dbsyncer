package org.dbsyncer.connector.oracle;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.oracle.geometry.JGeometry;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.ds.SimpleConnection;
import org.dbsyncer.connector.util.DatabaseUtil;
import org.dbsyncer.sdk.spi.ConnectorMapper;

import java.sql.Struct;

/**
 * JDBC索引{@link java.sql.Types 1111}, JDBC类型java.sql.Struct，支持的数据库类型：
 * <ol>
 * <li>用户定义的对象</li>
 * <li>VARCHAR2</li>
 * </ol>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/12/22 22:59
 */
public final class OracleOtherValueMapper extends AbstractValueMapper<Struct> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof java.sql.Struct || val instanceof String;
    }

    @Override
    protected Struct convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        // SqlServer Geometry
        if (val instanceof byte[]) {
            Object conn = connectorMapper.getConnection();
            if (conn instanceof SimpleConnection) {
                SimpleConnection connection = null;
                try {
                    connection = (SimpleConnection) conn;
                    OracleConnection oracleConnection = connection.unwrap(OracleConnection.class);
                    // TODO 兼容Oracle STRUCT 字节数组
                    Geometry geometry = Geometry.deserialize((byte[]) val);
                    Double x = geometry.getX();
                    Double y = geometry.getY();
                    JGeometry jGeometry = new JGeometry(x, y, 0);
                    return JGeometry.store(jGeometry, oracleConnection);
                } finally {
                    DatabaseUtil.close(connection);
                }
            }
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}