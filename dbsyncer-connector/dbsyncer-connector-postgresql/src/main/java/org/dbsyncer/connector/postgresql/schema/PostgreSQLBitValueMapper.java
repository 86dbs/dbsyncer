/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema;

import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-10 22:36
 */
@Deprecated
public class PostgreSQLBitValueMapper extends AbstractValueMapper<Boolean> {

    @Override
    protected Boolean convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof Integer) {
            Integer i = (Integer) val;
            return i == 1;
        }
        if (val instanceof Short) {
            Short s = (Short) val;
            return s == 1;
        }

        throw new PostgreSQLException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }

}