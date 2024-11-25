/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.schema.support;

import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.sql.Timestamp;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-21 23:56
 */
public class TimestampType extends AbstractDataType<Timestamp> {

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.TIMESTAMP;
    }
}
