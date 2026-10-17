/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.event;

import org.dbsyncer.common.message.impl.RemoveCacheMessage;
import org.springframework.context.ApplicationContext;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-09-23 01:14
 */
public final class RemoveMetaCacheEvent extends RemoveCacheEvent {

    public RemoveMetaCacheEvent(ApplicationContext source, RemoveCacheMessage commonMessage) {
        super(source, commonMessage);
    }
}
