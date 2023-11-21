/**
 * DBSyncer Copyright 2019-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.sdk.model.ChangedOffset;

import java.util.Map;

/**
 * 变更事件
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2023-08-20 20:00
 */
public interface ChangedEvent {

    /**
     * 获取变更表名称
     *
     * @return
     */
    String getSourceTableName();

    /**
     * 获取变更事件
     *
     * @return
     */
    String getEvent();

    /**
     * 获取变更行数据
     *
     * @return
     */
    Map<String, Object> getChangedRow();

    /**
     * 获取增量偏移量
     *
     * @return
     */
    ChangedOffset getChangedOffset();

}