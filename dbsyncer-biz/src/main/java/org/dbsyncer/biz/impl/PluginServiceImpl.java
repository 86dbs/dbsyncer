/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.PluginVO;
import org.dbsyncer.common.enums.FileSuffixEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ParserException;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Plugin;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
@Component
public class PluginServiceImpl implements PluginService {

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private LogService logService;

    @Override
    public List<PluginVO> getPluginAll() {
        List<Plugin> pluginAll = pluginFactory.getPluginAll();
        List<PluginVO> vos = new ArrayList<>();
        if (!CollectionUtils.isEmpty(pluginAll)) {
            Map<String, List<String>> pluginClassNameMap = getPluginClassNameMap();
            vos.addAll(pluginAll.stream().map(plugin-> {
                PluginVO vo = new PluginVO();
                BeanUtils.copyProperties(plugin, vo);
                vo.setMappingName(StringUtil.join(pluginClassNameMap.get(plugin.getClassName()), StringUtil.VERTICAL_LINE));
                return vo;
            }).collect(Collectors.toList()));
        }
        return vos;
    }

    @Override
    public String getPluginPath() {
        return pluginFactory.getPluginPath();
    }

    @Override
    public String getLibraryPath() {
        return pluginFactory.getLibraryPath();
    }

    @Override
    public void loadPlugins() {
        pluginFactory.loadPlugins();
        logService.log(LogType.PluginLog.UPDATE);
    }

    @Override
    public void checkFileSuffix(String filename) {
        Assert.hasText(filename, "the plugin filename is null.");
        String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
        FileSuffixEnum fileSuffix = FileSuffixEnum.getFileSuffix(suffix);
        Assert.notNull(fileSuffix, "Illegal file suffix");
        if (FileSuffixEnum.JAR != fileSuffix) {
            String msg = String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。", filename, FileSuffixEnum.JAR.getName());
            logService.log(LogType.PluginLog.CHECK_ERROR, msg);
            throw new BizException(msg);
        }
    }

    private Map<String, List<String>> getPluginClassNameMap() {
        Map<String, List<String>> map = new ConcurrentHashMap<>();
        taskProfile.pageScanTasks(Mapping.class, ConfigConstant.PAGE_SIZE, mappingAll -> {
            if (CollectionUtils.isEmpty(mappingAll)) {
                return;
            }
            for (Mapping m : mappingAll) {
                Plugin plugin = m.getPlugin();
                if (null != plugin) {
                    putPluginMap(map, plugin.getClassName(), m.getName());
                    continue;
                }

                AtomicBoolean pluginFound = new AtomicBoolean(false);
                tableGroupProfile.pageScanTableGroups(m.getId(), ConfigConstant.PAGE_SIZE, page -> {
                    if (pluginFound.get() || CollectionUtils.isEmpty(page)) {
                        return;
                    }
                    for (TableGroup t : page) {
                        if (t == null) {
                            continue;
                        }
                        Plugin p = t.getPlugin();
                        if (p != null) {
                            putPluginMap(map, p.getClassName(), m.getName());
                            pluginFound.set(true);
                            return;
                        }
                    }
                });
            }
        });
        return map;
    }

    private void putPluginMap(Map<String, List<String>> map, String className, String name) {
        map.compute(className, (k, v)-> {
            if (v == null) {
                try {
                    return new ArrayList<>();
                } catch (Exception e) {
                    throw new ParserException(e);
                }
            }
            return v;
        }).add(name);
    }
}