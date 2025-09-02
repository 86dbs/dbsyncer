package org.dbsyncer.common;

/**
 * 处理事件接口
 *
 * @author AE86
 * @version 1.0.0
 */
public interface ProcessEvent {

    /**
     * 任务完成处理
     *
     * @param metaId 元信息ID
     */
    void taskFinished(String metaId);

}