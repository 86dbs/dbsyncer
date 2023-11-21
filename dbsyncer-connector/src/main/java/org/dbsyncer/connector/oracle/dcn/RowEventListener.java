/**
 * DBSyncer Copyright 2019-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.dcn;

import org.dbsyncer.sdk.listener.event.RowChangedEvent;

/**
 * 行变更监听器
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-06-15 20:00
 */
public interface RowEventListener {

    void onEvents(RowChangedEvent rowChangedEvent);

}