package org.dbsyncer.manager;

import org.dbsyncer.parser.model.Mapping;

public interface Puller {

    void start(Mapping mapping);

    /**
     * 启动驱动。
     *
     * @param mapping      驱动
     * @param autoRecovery 是否为服务重启自动恢复（true 时对 CDC 监听启动失败按配置重试）
     */
    default void start(Mapping mapping, boolean autoRecovery) {
        start(mapping);
    }

    void close(String metaId);
}
