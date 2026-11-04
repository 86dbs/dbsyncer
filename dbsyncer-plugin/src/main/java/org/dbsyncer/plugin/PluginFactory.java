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
     * 上传 JAR 对应的 ClassLoader（按文件名）。
     */
    private final Map<String, URLClassLoader> classLoaders = new ConcurrentHashMap<>();

    /**
     * 加载用临时副本（key=原文件名）。ClassLoader 只打开副本，Windows 下 plugins 原文件可删/覆盖。
     */
    private final Map<String, File> loadCopies = new ConcurrentHashMap<>();

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
     * 删除上传的插件：先卸载内存（SPI + ClassLoader + 临时副本），再删 {@code plugins/} 原文件。
     * <p>仅删磁盘文件不够——运行中加载的是临时副本，不卸载则列表/转换仍可能命中旧实例。
     *
     * @param fileName 插件文件名（须为合法 jar 名）
     */
    public synchronized void deletePlugin(String fileName) {
        Assert.hasText(fileName, "插件文件名不能为空");
        if (!PluginFile.isSafeName(fileName)) {
            throw new IllegalArgumentException("插件文件名不合法，仅支持字母、数字、点、下划线和短横线");
        }
        File file = new File(PLUGIN_PATH, fileName);
        boolean known = classLoaders.containsKey(fileName) || loadCopies.containsKey(fileName);
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
        // 1) 卸掉已加载实例（含临时副本 ClassLoader）
        unloadJar(fileName);
        // 2) 再删 plugins 目录原文件（原文件未被 ClassLoader 打开，Windows 可删）
        if (file.exists()) {
            deleteFileWithRetry(file);
        }
        logger.info("已卸载并删除插件: {}", fileName);
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
     * SPI：扫描 jar 扩展实现。
     * <p>必须从临时副本加载：Windows 上 {@link URLClassLoader} 会锁住 jar，
     * 若直接加载 {@code plugins/} 原文件，不停止进程则无法手动/接口删除。
     *
     * @param jar plugins 目录下的原文件
     */
    private void loadPlugin(File jar) {
        String fileName = jar.getName();
        List<String> addedIds = new ArrayList<>();
        List<Plugin> addedPlugins = new ArrayList<>();
        List<PluginService> addedServices = new ArrayList<>();
        List<PluginService> replacedServices = new ArrayList<>();
        List<PluginService> replacedToClose = new ArrayList<>();
        try {
            unloadJar(fileName);
            File copy = File.createTempFile("dbs-plugin-", "-" + fileName);
            copy.deleteOnExit();
            FileUtils.copyFile(jar, copy);
            loadCopies.put(fileName, copy);
            URLClassLoader loader = new URLClassLoader(new URL[]{copy.toURI().toURL()},
                    Thread.currentThread().getContextClassLoader());
            classLoaders.put(fileName, loader);
            ServiceLoader<PluginService> services = ServiceLoader.load(PluginService.class, loader);
            boolean loaded = false;
            for (PluginService s : services) {
                String pluginId = createPluginId(s.getClass().getName(), s.getVersion());
                Plugin plugin = new Plugin(s.getName(), s.getClass().getName(), s.getVersion(), fileName, false);
                PluginService previous = service.put(pluginId, s);
                plugins.add(plugin);
                addedIds.add(pluginId);
                addedPlugins.add(plugin);
                addedServices.add(s);
                replacedServices.add(previous);
                logger.info("{}, {}_{} {}", fileName, s.getName(), s.getVersion(), s.getClass().getName());
                s.init();
                if (previous != null) {
                    replacedToClose.add(previous);
                }
                loaded = true;
            }
            for (PluginService previous : replacedToClose) {
                try {
                    previous.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            if (!loaded) {
                closeClassLoader(fileName);
                logger.warn("JAR 未发现 PluginService 实现: {}", fileName);
            }
        } catch (Throwable e) {
            rollbackLoaded(addedIds, addedPlugins, addedServices, replacedServices);
            closeClassLoader(fileName);
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 加载中途失败：摘掉本次注册，恢复被覆盖的实例，此时 ClassLoader 仍开着。
     */
    private void rollbackLoaded(List<String> addedIds, List<Plugin> addedPlugins, List<PluginService> addedServices,
                                List<PluginService> replacedServices) {
        for (int i = addedIds.size() - 1; i >= 0; i--) {
            plugins.remove(addedPlugins.get(i));
            String pluginId = addedIds.get(i);
            PluginService current = addedServices.get(i);
            PluginService previous = replacedServices.get(i);
            if (service.get(pluginId) == current) {
                if (previous != null) {
                    service.put(pluginId, previous);
                } else {
                    service.remove(pluginId);
                }
            }
            try {
                current.close();
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
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
        if (loader != null) {
            try {
                loader.close();
            } catch (IOException e) {
                logger.warn("关闭插件 ClassLoader 失败, file={}, err={}", fileName, e.getMessage());
            }
        }
        File copy = loadCopies.remove(fileName);
        if (copy == null || !copy.exists()) {
            return;
        }
        for (int i = 0; i < 3; i++) {
            if (copy.delete() || !copy.exists()) {
                return;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (copy.exists()) {
            copy.deleteOnExit();
            logger.warn("插件临时副本暂未删掉，已登记 JVM 退出时清理: {}", copy.getAbsolutePath());
        }
    }

    /**
     * 删除 plugins 原文件。若仍被占用则改名移出目录（不再被扫描加载）。
     */
    private void deleteFileWithRetry(File file) {
        final int attempts = 3;
        for (int i = 0; i < attempts; i++) {
            if (!file.exists()) {
                return;
            }
            if (file.delete()) {
                return;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!file.exists()) {
            return;
        }
        File tombstone = new File(file.getParentFile(), file.getName() + ".deleted." + System.currentTimeMillis());
        if (file.renameTo(tombstone)) {
            tombstone.deleteOnExit();
            logger.warn("插件文件仍被占用，已改名移出插件目录: {}", tombstone.getName());
            return;
        }
        throw new IllegalStateException("插件文件删除失败（可能仍被占用）: " + file.getName());
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
