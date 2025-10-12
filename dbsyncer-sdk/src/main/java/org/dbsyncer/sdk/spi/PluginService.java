/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.spi;

import org.dbsyncer.sdk.plugin.PluginContext;

/**
 * 插件扩展服务接口
 * <p>全量同步/增量同步,扩展转换</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/02/22 20:26
 */
public interface PluginService {

    /**
     * 初始化加载
     */
    default void init() {

    }

    /**
     * 全量同步/定时增量同步前置处理
     *
     * @param pluginContext 上下文
     */
    default void postProcessBefore(PluginContext pluginContext) {
    }

    /**
     * 全量同步/增量同步
     *
     * @param pluginContext 上下文
     */
    void convert(PluginContext pluginContext);

    /**
     * 全量同步/增量同步后置处理
     *
     * @param pluginContext 上下文
     */
    default void postProcessAfter(PluginContext pluginContext) {
    }

    /**
     * 版本号
     *
     * @return
     */
    default String getVersion() {
        return "1.0.0";
    }

    /**
     * 插件名称
     *
     * @return
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * 关闭服务(上传相同插件或服务关闭时触发)
     */
    default void close() {
    }
}