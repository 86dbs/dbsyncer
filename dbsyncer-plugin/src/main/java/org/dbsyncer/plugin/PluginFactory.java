package org.dbsyncer.plugin;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.plugin.model.Plugin;
import org.dbsyncer.sdk.spi.ConvertContext;
import org.dbsyncer.sdk.spi.ConvertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:26
 */
@Component
public class PluginFactory implements DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 插件路径dbsyncer/plugin/
     */
    private final String PLUGIN_PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("plugins")
            .append(File.separatorChar).toString();

    /**
     * 依赖路径dbsyncer/lib/
     */
    private final String LIBRARY_PATH = new StringBuilder(System.getProperty("user.dir")).append(File.separatorChar).append("lib")
            .append(File.separatorChar).toString();

    private final List<Plugin> plugins = new LinkedList<>();

    private final Map<String, ConvertService> service = new LinkedHashMap<>();

    @Resource
    private ApplicationContext applicationContext;

    @PostConstruct
    private void init() {
        Map<String, ConvertService> services = applicationContext.getBeansOfType(ConvertService.class);
        if (!CollectionUtils.isEmpty(services)) {
            services.forEach((k, s) -> {
                String pluginId = createPluginId(s.getClass().getName(), s.getVersion());
                service.putIfAbsent(pluginId, s);
                plugins.add(new Plugin(s.getName(), s.getClass().getName(), s.getVersion(), "", true));
                logger.info("{}_{} {}", s.getName(), s.getVersion(), s.getClass().getName());
                try {
                    s.init();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        }
    }

    public synchronized void loadPlugins() {
        if (!CollectionUtils.isEmpty(plugins)) {
            List<Plugin> unmodifiablePlugin = plugins.stream().filter(p -> p.isUnmodifiable()).collect(Collectors.toList());
            plugins.clear();
            plugins.addAll(unmodifiablePlugin);
        }
        try {
            FileUtils.forceMkdir(new File(PLUGIN_PATH));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        Collection<File> files = FileUtils.listFiles(new File(PLUGIN_PATH), new String[]{"jar"}, true);
        if (!CollectionUtils.isEmpty(files)) {
            files.forEach(f -> loadPlugin(f));
        }
        logger.info("PreLoad plugin:{}", plugins.size());
    }

    public String getPluginPath() {
        return PLUGIN_PATH;
    }

    public String getLibraryPath() {
        return LIBRARY_PATH;
    }

    public List<Plugin> getPluginAll() {
        return Collections.unmodifiableList(plugins);
    }

    /**
     * 全量同步/增量同步
     *
     * @param plugin
     * @param context
     */
    public void convert(Plugin plugin, ConvertContext context) {
        if (null != plugin) {
            String pluginId = createPluginId(plugin.getClassName(), plugin.getVersion());
            service.computeIfPresent(pluginId, (k, c) -> {
                c.convert(context);
                return c;
            });
        }
    }

    /**
     * 全量同步/增量同步完成后执行处理
     *
     * @param plugin
     * @param context
     */
    public void postProcessAfter(Plugin plugin, ConvertContext context) {
        if (null != plugin) {
            String pluginId = createPluginId(plugin.getClassName(), plugin.getVersion());
            service.computeIfPresent(pluginId, (k, c) -> {
                c.postProcessAfter(context);
                return c;
            });
        }
    }

    public String createPluginId(String pluginClassName, String pluginVersion) {
        return new StringBuilder(pluginClassName).append("_").append(pluginVersion).toString();
    }

    /**
     * SPI, 扫描jar扩展接口实现，注册为本地服务
     *
     * @param jar
     */
    private void loadPlugin(File jar) {
        try {
            String fileName = jar.getName();
            URL url = jar.toURI().toURL();
            URLClassLoader loader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
            ServiceLoader<ConvertService> services = ServiceLoader.load(ConvertService.class, loader);
            for (ConvertService s : services) {
                String pluginId = createPluginId(s.getClass().getName(), s.getVersion());
                // 先释放历史版本
                if (service.containsKey(pluginId)) {
                    try {
                        service.get(pluginId).close();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    service.remove(pluginId);
                }
                service.putIfAbsent(pluginId, s);
                plugins.add(new Plugin(s.getName(), s.getClass().getName(), s.getVersion(), fileName, false));
                logger.info("{}, {}_{} {}", fileName, s.getName(), s.getVersion(), s.getClass().getName());
                try {
                    s.init();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void destroy() {
        service.values().forEach(s -> {
            logger.info("{}_{} {}", s.getName(), s.getVersion(), s.getClass().getName());
            try {
                s.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        });
        service.clear();
    }
}