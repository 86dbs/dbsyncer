package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.postgis.Geometry;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;

import java.sql.Struct;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/9/16 16:54
 */
public class OtherValueMapper extends AbstractValueMapper<Object> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.STRUCT || val instanceof String;
    }

    @Override
    protected Object convert(ConnectorMapper connectorMapper, Object val) throws Exception {

        if (val instanceof oracle.sql.STRUCT) {
            return (Struct) val;
        }

        //PostGIS Geometry的情况
        if (val instanceof String)
        {
            //测试下Geometry能不能用起来
            try
            {
                BinaryParser parser= new BinaryParser();
                org.postgis.Geometry geo = parser.parse((String) val);
                BinaryWriter bw = new BinaryWriter();
                return bw.writeBinary(geo);
            }
            catch (Exception ex) {
                System.out.println(val);
                return val;
            }
        }
        if (val instanceof Geometry)
        {
            return val;

        }

        // SqlServer Geometry

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}