package org.dbsyncer.common.event;

import org.dbsyncer.common.message.impl.RemoveCacheMessage;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 删除配置缓存事件
 *
 * @version 1.0.0
 * @author AE86
 * @date 2020-04-26 22:45
 */
public class RemoveCacheEvent extends ApplicationContextEvent {

    private final RemoveCacheMessage commonMessage;

    public RemoveCacheEvent(ApplicationContext source, RemoveCacheMessage commonMessage) {
        super(source);
        this.commonMessage = commonMessage;
    }

    public RemoveCacheMessage getCommonMessage() {
        return commonMessage;
    }
}
