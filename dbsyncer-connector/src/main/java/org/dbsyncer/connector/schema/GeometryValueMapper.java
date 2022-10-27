package org.dbsyncer.connector.schema;

import com.microsoft.sqlserver.jdbc.Geometry;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class GeometryValueMapper extends AbstractValueMapper<Geometry> {

    @Override
    protected Object getDefaultVal(Object val) throws SQLServerException {
        return null != val ? val : Geometry.point(0, 0, 0);
    }

    @Override
    protected Geometry convert(ConnectorMapper connectorMapper, Object val) throws SQLServerException {
        if (val instanceof byte[]) {
            return Geometry.deserialize((byte[]) val);
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}