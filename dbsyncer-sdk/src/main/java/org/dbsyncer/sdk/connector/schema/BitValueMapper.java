package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
public class BitValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected byte[] convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof BitSet) {
            BitSet bitSet = (BitSet) val;
            return bitSet.toByteArray();
        }
        if (val instanceof Integer) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt((Integer) val);
            return buffer.array();
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort((short) (b ? 1 : 0));
            return buffer.array();
        }
        // 添加对Byte类型的支持
        if (val instanceof Byte) {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            buffer.put((Byte) val);
            return buffer.array();
        }

        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}