package org.dbsyncer.listener.config;

import org.dbsyncer.listener.enums.ListenerEnum;

/**
 * 日志配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/7 17:08
 */
public class LogListenerConfig extends ListenerConfig {

    public LogListenerConfig() {
        setListenerType(ListenerEnum.LOG.getType());
    }

    // 表别名
    private String tableLabel = "";

    public String getTableLabel() {
        return tableLabel;
    }

    public LogListenerConfig setTableLabel(String tableLabel) {
        this.tableLabel = tableLabel;
        return this;
    }
    
}