package org.dbsyncer.plugin;

import org.dbsyncer.plugin.config.Plugin;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:26
 */
@Component
public class PluginFactory {

    public List<Plugin> getPluginAll() {
        List<Plugin> list = new ArrayList<>();
        Plugin plugin = new Plugin();
        plugin.setName("正式知识库插件");
        plugin.setClassName("com.knowledge.xx.impl");
        list.add(plugin);
        return list;
    }

}