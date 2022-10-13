package org.dbsyncer.connector.schema;

import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.ConnectorMapper;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class BitValueMapper extends AbstractValueMapper<byte[]> {

    private static final ByteBuffer buffer = ByteBuffer.allocate(4);

    @Override
    protected byte[] convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            return bitSet.toByteArray();
        }
        if (val instanceof Integer) {
            synchronized (this){
                buffer.clear();
                buffer.putInt((Integer) val);
                buffer.flip();
                byte[] bytes = new byte[4];
                buffer.get(bytes);
                return bytes;
            }
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            synchronized (this){
                buffer.clear();
                buffer.putShort((short) (b ? 1 : 0));
                buffer.flip();
                byte[] bytes = new byte[2];
                buffer.get(bytes);
                return bytes;
            }
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}