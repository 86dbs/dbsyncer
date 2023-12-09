/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener.event;

import org.dbsyncer.sdk.enums.ChangedEventTypeEnum;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:34
 */
public final class SqlChangedEvent extends CommonChangedEvent {

    public SqlChangedEvent(String sourceTableName, String event, String sql, String nextFileName, Object position) {
        setSourceTableName(sourceTableName);
        setEvent(event);
        setNextFileName(nextFileName);
        setPosition(position);
        setSql(sql);
    }

    @Override
    public ChangedEventTypeEnum getType() {
        return ChangedEventTypeEnum.SQL;
    }
}