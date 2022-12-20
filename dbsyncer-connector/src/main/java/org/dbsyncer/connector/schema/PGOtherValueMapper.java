package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.postgis.Geometry;
import org.postgis.PGgeometry;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;

public class PGOtherValueMapper extends AbstractValueMapper<Object> {
    @Override
    protected Object convert(ConnectorMapper connectorMapper, Object val) throws Exception {
        if (val instanceof String) {
            //测试下Geometry能不能用起来
            try {
                BinaryParser parser = new BinaryParser();
                org.postgis.Geometry geo = parser.parse((String) val);
                BinaryWriter bw = new BinaryWriter();
                return bw.writeBinary(geo);
            } catch (Exception ex) {
                return val;
            }
        } else if (val instanceof PGgeometry)
            return val;
        else if (val instanceof Geometry) {
            return val;
        } else if (val instanceof java.util.UUID) {
            return val;
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}
