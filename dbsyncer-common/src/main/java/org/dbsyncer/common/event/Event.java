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
     * 数据变更事件
     *
     * @param tableName 表名
     * @param event     事件
     * @param before    变化前
     * @param after     变化后
     */
    void changedEvent(String tableName, String event, List<Object> before, List<Object> after);

    /**
     * 写入增量点事件
     *
     * @param map
     */
    void flushEvent(Map<String, String> map);

}