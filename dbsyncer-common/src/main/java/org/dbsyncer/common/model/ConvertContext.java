package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ProxyApplicationContext;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:00
 */
public class ConvertContext {

    /**
     * Spring上下文
     */
    protected ProxyApplicationContext context;

    /**
     * 目标表
     */
    protected String targetTableName;

    public ProxyApplicationContext getContext() {
        return context;
    }

    public String getTargetTableName() {
        return targetTableName;
    }
}