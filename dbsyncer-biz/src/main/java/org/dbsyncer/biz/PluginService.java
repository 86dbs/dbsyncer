package org.dbsyncer.biz;

import org.dbsyncer.plugin.config.Plugin;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
public interface PluginService {

    /**
     * 获取所有插件
     *
     * @return
     */
    List<Plugin> getPluginAll();

}