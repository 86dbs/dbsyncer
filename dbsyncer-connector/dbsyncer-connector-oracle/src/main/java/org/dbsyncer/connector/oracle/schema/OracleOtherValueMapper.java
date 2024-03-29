/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.schema;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
import org.dbsyncer.connector.oracle.OracleException;
import org.dbsyncer.connector.oracle.geometry.JGeometry;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.database.ds.SimpleConnection;
import org.dbsyncer.sdk.util.DatabaseUtil;

import java.sql.Struct;

/**
 * JDBC索引{@link java.sql.Types 1111}, JDBC类型java.sql.Struct，支持的数据库类型：
 * <ol>
 * <li>用户定义的对象</li>
 * <li>VARCHAR2</li>
 * </ol>
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-12-22 22:59
 */
public final class OracleOtherValueMapper extends AbstractValueMapper<Struct> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof java.sql.Struct || val instanceof String;
    }

    @Override
    protected Struct convert(ConnectorInstance connectorInstance, Object val) throws Exception {
        // SqlServer Geometry
        if (val instanceof byte[]) {
            Object conn = connectorInstance.getConnection();
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
        throw new OracleException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}