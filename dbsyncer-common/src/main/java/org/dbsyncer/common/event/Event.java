package org.dbsyncer.common.event;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-11 22:50
 */
public interface Event {

    /**
     * 日志数据变更事件
     *
     * @param tableName 表名
     * @param event     事件
     * @param before    变化前
     * @param after     变化后
     */
    void changedLogEvent(String tableName, String event, List<Object> before, List<Object> after);

    /**
     * 定时数据变更事件
     *
     * @param tableGroupIndex
     * @param event
     * @param before
     * @param after
     */
    void changedQuartzEvent(int tableGroupIndex, String event, Map<String, Object> before, Map<String, Object> after);

    /**
     * 写入增量点事件
     *
     * @param map
     */
    void flushEvent(Map<String, String> map);

    /**
     * 异常事件
     *
     * @param e
     */
    void errorEvent(Exception e);

}