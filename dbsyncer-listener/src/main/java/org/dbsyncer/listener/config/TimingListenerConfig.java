package org.dbsyncer.listener.config;

import org.dbsyncer.listener.enums.ListenerEnum;

/**
 * 定时配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/7 14:08
 */
public class TimingListenerConfig extends ListenerConfig {

    public TimingListenerConfig() {
        setListenerType(ListenerEnum.POLLING.getType());
    }

    // 定时表达式, 格式: [秒] [分] [小时] [日] [月] [周]
    private String cronExpression = "*/30 * * * * ?";

    // 事件字段
    private String eventFieldName = "";

    // 修改事件, 例如当eventFieldName值等于U 或 update时，判定该条数据为修改操作
    private String update = "U";

    // 插入事件
    private String insert = "I";

    // 删除事件
    private String delete = "D";

    public String getCronExpression() {
        return cronExpression;
    }

    public TimingListenerConfig setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
        return this;
    }

    public String getEventFieldName() {
        return eventFieldName;
    }

    public TimingListenerConfig setEventFieldName(String eventFieldName) {
        this.eventFieldName = eventFieldName;
        return this;
    }

    public String getUpdate() {
        return update;
    }

    public TimingListenerConfig setUpdate(String update) {
        this.update = update;
        return this;
    }

    public String getInsert() {
        return insert;
    }

    public TimingListenerConfig setInsert(String insert) {
        this.insert = insert;
        return this;
    }

    public String getDelete() {
        return delete;
    }

    public TimingListenerConfig setDelete(String delete) {
        this.delete = delete;
        return this;
    }

}