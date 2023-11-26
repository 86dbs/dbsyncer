/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.dcn;

import org.dbsyncer.sdk.listener.event.RowChangedEvent;

/**
 * 行变更监听器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-06-08 21:53
 */
public interface RowEventListener {

    void onEvents(RowChangedEvent rowChangedEvent);

}