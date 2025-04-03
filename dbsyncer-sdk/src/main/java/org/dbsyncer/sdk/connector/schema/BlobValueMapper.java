package org.dbsyncer.sdk.connector.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/25 0:07
 */
@Deprecated
public class BlobValueMapper extends AbstractValueMapper<byte[]> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof oracle.sql.BLOB || val instanceof byte[];
    }

    @Override
    protected byte[] convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            String s = (String) val;
            if (s.startsWith("HEXTORAW(")) {
                return StringUtil.hexStringToByteArray(s.replace("HEXTORAW('", "").replace("')", ""));
            }
            if ("EMPTY_BLOB()".equals(s)) {
                return null;
            }
            return s.getBytes();
        }
        throw new SdkException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}
