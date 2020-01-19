package org.dbsyncer.listener.config;

import org.dbsyncer.connector.config.Filter;
import org.dbsyncer.listener.enums.ListenerEnum;

import java.util.List;

/**
 * 轮询配置
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/7 14:08
 */
public class PollingListenerConfig extends ListenerConfig {

    public PollingListenerConfig() {
        setListenerType(ListenerEnum.POLLING.getType());
    }

    // 定时表达式, 格式: [秒] [分] [小时] [日] [月] [周]
    private String cronExpression = "*/30 * * * * ?";

    // 过滤条件
    private Filter filter;

    // 事件字段
    private String eventFieldName;

    // 修改事件, 例如当eventFieldName值等于U 或 update时，判定该条数据为修改操作
    private List<String> update;

    // 插入事件
    private List<String> insert;

    // 删除事件
    private List<String> delete;

    public String getCronExpression() {
        return cronExpression;
    }

    public PollingListenerConfig setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
        return this;
    }

    public Filter getFilter() {
        return filter;
    }

    public PollingListenerConfig setFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public String getEventFieldName() {
        return eventFieldName;
    }

    public PollingListenerConfig setEventFieldName(String eventFieldName) {
        this.eventFieldName = eventFieldName;
        return this;
    }

    public List<String> getUpdate() {
        return update;
    }

    public PollingListenerConfig setUpdate(List<String> update) {
        this.update = update;
        return this;
    }

    public List<String> getInsert() {
        return insert;
    }

    public PollingListenerConfig setInsert(List<String> insert) {
        this.insert = insert;
        return this;
    }

    public List<String> getDelete() {
        return delete;
    }

    public PollingListenerConfig setDelete(List<String> delete) {
        this.delete = delete;
        return this;
    }

}