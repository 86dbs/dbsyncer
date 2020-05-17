package org.dbsyncer.plugin;

import org.dbsyncer.plugin.config.Plugin;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public void convert(Plugin plugin, List<Map<String, Object>> source, List<Map<String, Object>> target) {
        if (null != plugin) {
            // TODO 插件转换
        }
    }

    public void convert(Plugin plugin, String event, Map<String, Object> source, Map<String, Object> target) {
        if (null != plugin) {
        }
    }
}