package org.dbsyncer.connector.schema;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 23:43
 */
public class VarBinaryValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected Object getDefaultVal(Object val) {
        return null != val ? val : new byte[0];
    }

    @Override
    protected byte[] convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            return toByteArray(bitSet);
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort((short) (b ? 1 : 0));
            return buffer.array();
        }
        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

    public byte[] toByteArray(BitSet bits) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
            }
        }
        return bytes;
    }
}