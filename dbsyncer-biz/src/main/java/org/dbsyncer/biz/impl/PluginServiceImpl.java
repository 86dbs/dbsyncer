package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.PluginService;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.plugin.config.Plugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
@Component
public class PluginServiceImpl implements PluginService {

    @Autowired
    private Manager manager;

    @Override
    public List<Plugin> getPluginAll() {
        return manager.getPluginAll();
    }

    @Override
    public String getPluginPath() {
        return manager.getPluginPath();
    }

    @Override
    public void loadPlugins() {
        manager.loadPlugins();
    }
}