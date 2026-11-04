/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.plugin;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.plugin.enums.ProcessEnum;
import org.dbsyncer.sdk.model.Plugin;
import org.dbsyncer.sdk.model.PluginFile;
import org.dbsyncer.sdk.plugin.PluginContext;
import org.dbsyncer.sdk.spi.PluginService;

import org.apache.commons.io.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final String PLUGIN_PATH = System.getProperty("user.dir") + File.separatorChar + "plugins" + File.separatorChar;

    /**
     * 依赖路径dbsyncer/lib/
     */
    private final String LIBRARY_PATH = System.getProperty("user.dir") + File.separatorChar + "lib" + File.separatorChar;

    private final List<Plugin> plugins = new LinkedList<>();

    private final Map<String, PluginService> service = new ConcurrentHashMap<>();

    /**
     * 上传 JAR 对应的 ClassLoader（按文件名）。Windows 须先 close 才能删/覆盖文件。
     */
    private final Map<String, URLClassLoader> classLoaders = new ConcurrentHashMap<>();

    @Resource
    private ApplicationContext applicationContext;

    @PostConstruct
    private void init() {
        Map<String, PluginService> services = applicationContext.getBeansOfType(PluginService.class);
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
        // 先释放已加载的上传插件，避免 Windows 下 JAR 文件锁导致无法覆盖/删除
        unloadAllUploaded();
        if (!CollectionUtils.isEmpty(plugins)) {
            List<Plugin> unmodifiablePlugin = plugins.stream().filter(Plugin::isUnmodifiable).collect(Collectors.toList());
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
            files.forEach(this::loadPlugin);
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
     * 仅卸载内存中的上传插件（关 SPI + ClassLoader），不删文件。覆盖上传前调用。
     *
     * @param fileName 插件文件名
     */
    public synchronized void unloadPlugin(String fileName) {
        Assert.hasText(fileName, "插件文件名不能为空");
        if (!PluginFile.isSafeName(fileName)) {
            throw new IllegalArgumentException("插件文件名不合法，仅支持字母、数字、点、下划线和短横线");
        }
        unloadJar(fileName);
    }

    /**
     * 删除上传的插件 JAR：关闭 SPI/ClassLoader 并删除文件。内置插件不可删。
     *
     * @param fileName 插件文件名（须为合法 jar 名）
     */
    public synchronized void deletePlugin(String fileName) {
        Assert.hasText(fileName, "插件文件名不能为空");
        if (!PluginFile.isSafeName(fileName)) {
            throw new IllegalArgumentException("插件文件名不合法，仅支持字母、数字、点、下划线和短横线");
        }
        File file = new File(PLUGIN_PATH, fileName);
        boolean known = classLoaders.containsKey(fileName);
        for (Plugin plugin : plugins) {
            if (StringUtil.equals(fileName, plugin.getFileName())) {
                known = true;
                if (plugin.isUnmodifiable()) {
                    throw new IllegalArgumentException("内置插件不能删除");
                }
            }
        }
        if (!file.exists() && !known) {
            throw new IllegalArgumentException("插件文件不存在: " + fileName);
        }
        unloadJar(fileName);
        if (file.exists()) {
            deleteFileWithRetry(file);
        }
        logger.info("已删除插件文件: {}", fileName);
    }

    /**
     * 全量同步/增量同步处理
     *
     * @param context
     */
    public void process(PluginContext context, ProcessEnum processEnum) {
        Plugin plugin = context.getPlugin();
        if (null != plugin) {
            String pluginId = createPluginId(plugin.getClassName(), plugin.getVersion());
            service.computeIfPresent(pluginId, (k, c) -> {
                switch (processEnum) {
                    case BEFORE:
                        c.postProcessBefore(context);
                        break;
                    case CONVERT:
                        c.convert(context);
                        break;
                    case AFTER:
                        c.postProcessAfter(context);
                        break;
                }
                return c;
            });
        }
    }

    public String createPluginId(String pluginClassName, String pluginVersion) {
        return pluginClassName + "_" + pluginVersion;
    }

    /**
     * SPI, 扫描jar扩展接口实现，注册为本地服务
     *
     * @param jar
     */
    private void loadPlugin(File jar) {
        String fileName = jar.getName();
        URLClassLoader loader = null;
        try {
            unloadJar(fileName);
            URL url = jar.toURI().toURL();
            loader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
            classLoaders.put(fileName, loader);
            ServiceLoader<PluginService> services = ServiceLoader.load(PluginService.class, loader);
            boolean loaded = false;
            for (PluginService s : services) {
                String pluginId = createPluginId(s.getClass().getName(), s.getVersion());
                PluginService previous = service.put(pluginId, s);
                if (previous != null) {
                    try {
                        previous.close();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
                plugins.add(new Plugin(s.getName(), s.getClass().getName(), s.getVersion(), fileName, false));
                logger.info("{}, {}_{} {}", fileName, s.getName(), s.getVersion(), s.getClass().getName());
                s.init();
                loaded = true;
            }
            if (!loaded) {
                closeClassLoader(fileName);
                logger.warn("JAR 未发现 PluginService 实现: {}", fileName);
            }
        } catch (Throwable e) {
            closeClassLoader(fileName);
            logger.error(e.getMessage(), e);
        }
    }

    private void unloadAllUploaded() {
        Set<String> fileNames = new HashSet<>(classLoaders.keySet());
        for (Plugin plugin : plugins) {
            if (!plugin.isUnmodifiable() && StringUtil.isNotBlank(plugin.getFileName())) {
                fileNames.add(plugin.getFileName());
            }
        }
        for (String fileName : fileNames) {
            unloadJar(fileName);
        }
    }

    private void unloadJar(String fileName) {
        List<Plugin> matched = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (StringUtil.equals(fileName, plugin.getFileName())) {
                matched.add(plugin);
            }
        }
        for (Plugin plugin : matched) {
            String pluginId = createPluginId(plugin.getClassName(), plugin.getVersion());
            PluginService pluginService = service.remove(pluginId);
            if (pluginService != null) {
                try {
                    pluginService.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            plugins.remove(plugin);
        }
        closeClassLoader(fileName);
    }

    private void closeClassLoader(String fileName) {
        URLClassLoader loader = classLoaders.remove(fileName);
        if (loader == null) {
            return;
        }
        try {
            loader.close();
        } catch (IOException e) {
            logger.warn("关闭插件 ClassLoader 失败, file={}, err={}", fileName, e.getMessage());
        }
    }

    /**
     * Windows 在 ClassLoader.close 后偶发仍短暂占用文件，短重试后仍失败则抛错。
     */
    private void deleteFileWithRetry(File file) {
        final int attempts = 5;
        for (int i = 0; i < attempts; i++) {
            if (!file.exists()) {
                return;
            }
            if (file.delete()) {
                return;
            }
            System.gc();
            try {
                Thread.sleep(100L * (i + 1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (file.exists()) {
            throw new IllegalStateException("插件文件删除失败（可能仍被占用）: " + file.getName());
        }
    }

    @Override
    public void destroy() {
        unloadAllUploaded();
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
