/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.listener;

import org.dbsyncer.common.event.ClosedEvent;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.enums.MetaEnum;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-14 01:48
 */
@Component
public class ClosedTaskListener implements ApplicationListener<ClosedEvent> {

    @Resource
    private ManagerFactory managerFactory;

    @Override
    public void onApplicationEvent(ClosedEvent event) {
        managerFactory.changeMetaState(event.getMetaId(), MetaEnum.READY);
    }
}