package org.dbsyncer.biz.impl;

import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

public class BaseServiceImpl {

    @Autowired
    protected Manager manager;

    /**
     * 驱动启停锁
     */
    protected final static Object LOCK = new Object();

    protected boolean isRunning(String metaId) {
        Meta meta = manager.getMeta(metaId);
        if (null != meta) {
            int state = meta.getState();
            return MetaEnum.isRunning(state);
        }
        return false;
    }

    protected void assertRunning(String metaId) {
        Assert.isTrue(!isRunning(metaId), "驱动正在运行, 请先停止.");
    }

    protected void assertRunning(TableGroup model) {
        synchronized (LOCK) {
            Mapping mapping = manager.getMapping(model.getMappingId());
            assertRunning(mapping.getMetaId());
        }
    }

}