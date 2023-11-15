package org.dbsyncer.connector.file;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.connector.AbstractValueMapper;
import org.dbsyncer.connector.ConnectorException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/11/15 0:07
 */
public class FileBitValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorMapper connectorMapper, Object val) {
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return b ? 1 : 0;
        }

        throw new ConnectorException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}