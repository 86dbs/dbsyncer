package org.dbsyncer.connector.oracle;

import com.microsoft.sqlserver.jdbc.Geometry;
import oracle.jdbc.OracleConnection;
import oracle.spatial.geometry.JGeometry;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.ds.SimpleConnection;

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
            SimpleConnection connection = (SimpleConnection) connectorMapper.getConnection();
            OracleConnection conn = connection.unwrap(OracleConnection.class);
            // TODO 兼容Oracle STRUCT 字节数组
            Geometry geometry = Geometry.deserialize((byte[]) val);
            Double x = geometry.getX();
            Double y = geometry.getY();
            JGeometry jGeometry = new JGeometry(x, y, 0);
            return JGeometry.store(jGeometry, conn);
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}