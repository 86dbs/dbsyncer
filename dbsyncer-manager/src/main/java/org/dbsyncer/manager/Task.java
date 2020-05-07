package org.dbsyncer.manager;

import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.model.Mapping;

/**
 * 同步任务执行器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 16:32
 */
public interface Task {

    /**
     * 启动同步任务
     *
     * @param mapping
     */
    void start(Mapping mapping);

    /**
     * 关闭同步任务
     *
     * @param mapping
     */
    void close(Mapping mapping);

    /**
     * 切换meta状态
     *
     * @param metaId
     * @param metaEnum
     */
    void changeMetaState(String metaId, MetaEnum metaEnum);

}