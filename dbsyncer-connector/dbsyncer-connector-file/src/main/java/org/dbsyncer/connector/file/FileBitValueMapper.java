/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file;

import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/11/15 0:07
 */
public class FileBitValueMapper extends AbstractValueMapper<Integer> {

    @Override
    protected Integer convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return b ? 1 : 0;
        }

        throw new FileException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}