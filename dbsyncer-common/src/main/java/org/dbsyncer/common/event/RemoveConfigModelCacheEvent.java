package org.dbsyncer.common.event;

import org.dbsyncer.common.message.impl.RemoveConfigModelCacheMessage;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ApplicationContextEvent;

/**
 * 删除配置缓存事件
 *
 * @version 1.0.0
 * @author AE86
 * @date 2020-04-26 22:45
 */
public class RemoveConfigModelCacheEvent extends ApplicationContextEvent {

    private final RemoveConfigModelCacheMessage commonMessage;

    public RemoveConfigModelCacheEvent(ApplicationContext source, RemoveConfigModelCacheMessage commonMessage) {
        super(source);
        this.commonMessage = commonMessage;
    }

    public RemoveConfigModelCacheMessage getCommonMessage() {
        return commonMessage;
    }
}
