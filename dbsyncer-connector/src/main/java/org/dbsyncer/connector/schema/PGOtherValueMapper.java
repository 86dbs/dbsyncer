package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.postgis.Geometry;
import org.postgis.PGgeometry;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;

/**
 * PostgreSQL值映射器，JDBC索引{@link java.sql.Types 1111}, JDBC类型java.lang.Object，支持的数据库类型：
 * <ol>
 * <li>cidr</li>
 * <li>inet</li>
 * <li>macaddr</li>
 * <li>box</li>
 * <li>circle</li>
 * <li>interval</li>
 * <li>line</li>
 * <li>lseg</li>
 * <li>path</li>
 * <li>point</li>
 * <li>polygon</li>
 * <li>varbit</li>
 * </ol>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class PGOtherValueMapper extends AbstractValueMapper<Object> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof PGgeometry || val instanceof Geometry || val instanceof java.util.UUID;
    }

    @Override
    protected Object convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof String) {
            try {
                BinaryParser parser = new BinaryParser();
                org.postgis.Geometry geo = parser.parse((String) val);
                BinaryWriter bw = new BinaryWriter();
                return bw.writeBinary(geo);
            } catch (Exception ex) {
                return val;
            }
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}