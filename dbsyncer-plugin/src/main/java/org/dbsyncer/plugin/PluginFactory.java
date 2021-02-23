package org.dbsyncer.plugin;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.spi.ConvertService;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.plugin.config.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:26
 */
@Component
public class PluginFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 插件路径dbsyncer/plugin/
     */
    private final String PLUGIN_PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("plugins")
            .append(File.separatorChar).toString();

    private final List<Plugin> plugins = new LinkedList<>();

    private final Map<String, ConvertService> service = new ConcurrentHashMap<>();

    public synchronized void loadPlugins() {
        plugins.clear();
        service.clear();
        try {
            FileUtils.forceMkdir(new File(PLUGIN_PATH));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        Collection<File> files = FileUtils.listFiles(new File(PLUGIN_PATH), new String[] {"jar"}, true);
        if (!CollectionUtils.isEmpty(files)) {
            files.forEach(f -> loadPlugin(f));
        }
        logger.info("PreLoad plugin:{}", plugins.size());
    }

    public String getPluginPath() {
        return PLUGIN_PATH;
    }

    public List<Plugin> getPluginAll() {
        return plugins;
    }

    public void convert(Plugin plugin, List<Map<String, Object>> source, List<Map<String, Object>> target) {
        if (null != plugin && service.containsKey(plugin.getClassName())) {
            service.get(plugin.getClassName()).convert(source, target);
        }
    }

    public void convert(Plugin plugin, String event, Map<String, Object> source, Map<String, Object> target) {
        if (null != plugin && service.containsKey(plugin.getClassName())) {
            service.get(plugin.getClassName()).convert(event, source, target);
        }
    }

    /**
     * SPI, 扫描jar扩展接口实现，注册为本地服务
     *
     * @param jar
     */
    private void loadPlugin(File jar) {
        try {
            URL url = jar.toURI().toURL();
            URLClassLoader loader = new URLClassLoader(new URL[] {url}, Thread.currentThread().getContextClassLoader());
            ServiceLoader<ConvertService> services = ServiceLoader.load(ConvertService.class, loader);
            for (ConvertService s : services) {
                String className = s.getClass().getName();
                service.put(className, s);
                plugins.add(new Plugin(s.getName() + "_" + s.getVersion(), className, s.getVersion()));
            }
        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        }

    }

}