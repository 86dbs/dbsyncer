package org.dbsyncer.parser.model;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/8 22:36
 */
public class ListenerConfig {

    /**
     * 监听器类型
     */
    private String listenerType;

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

    // 表别名
    private String tableLabel = "T1";

    public ListenerConfig() {
    }

    public ListenerConfig(String listenerType) {
        this.listenerType = listenerType;
    }

    public String getListenerType() {
        return listenerType;
    }

    public void setListenerType(String listenerType) {
        this.listenerType = listenerType;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getEventFieldName() {
        return eventFieldName;
    }

    public void setEventFieldName(String eventFieldName) {
        this.eventFieldName = eventFieldName;
    }

    public String getUpdate() {
        return update;
    }

    public void setUpdate(String update) {
        this.update = update;
    }

    public String getInsert() {
        return insert;
    }

    public void setInsert(String insert) {
        this.insert = insert;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getTableLabel() {
        return tableLabel;
    }

    public void setTableLabel(String tableLabel) {
        this.tableLabel = tableLabel;
    }
}