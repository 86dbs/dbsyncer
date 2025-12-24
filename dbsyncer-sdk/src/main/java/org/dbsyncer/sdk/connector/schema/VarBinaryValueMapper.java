package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

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
    protected byte[] convert(ConnectorInstance connectorInstance, Object val) {
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
        if (val instanceof String) {
            String s = (String) val;
            // 处理 Oracle HEXTORAW 格式: HEXTORAW('30303030303030303030303030303030')
            // 支持多种格式: HEXTORAW('...'), HEXTORAW("..."), HEXTORAW(...)
            if (s.trim().toUpperCase().startsWith("HEXTORAW(")) {
                // 提取括号内的内容
                int startIdx = s.indexOf('(');
                int endIdx = s.lastIndexOf(')');
                if (startIdx >= 0 && endIdx > startIdx) {
                    String hexContent = s.substring(startIdx + 1, endIdx).trim();
                    // 移除引号（单引号或双引号）
                    if ((hexContent.startsWith("'") && hexContent.endsWith("'")) ||
                        (hexContent.startsWith("\"") && hexContent.endsWith("\""))) {
                        hexContent = hexContent.substring(1, hexContent.length() - 1);
                    }
                    return StringUtil.hexStringToByteArray(hexContent);
                }
            }
            // 处理纯十六进制字符串（只包含 0-9, A-F, a-f）
            if (isHexString(s)) {
                return StringUtil.hexStringToByteArray(s);
            }
            // 普通字符串转换为字节数组
            return s.getBytes();
        }
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

    /**
     * 判断字符串是否为十六进制字符串
     */
    private boolean isHexString(String str) {
        if (StringUtil.isBlank(str)) {
            return false;
        }
        // 十六进制字符串长度必须是偶数
        if (str.length() % 2 != 0) {
            return false;
        }
        // 检查是否只包含十六进制字符
        for (char c : str.toCharArray()) {
            if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
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