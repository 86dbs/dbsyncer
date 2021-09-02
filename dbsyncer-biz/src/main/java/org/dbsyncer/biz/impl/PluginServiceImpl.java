package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.PluginVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.plugin.enums.FileSuffixEnum;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
@Component
public class PluginServiceImpl implements PluginService {

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Override
    public List<PluginVo> getPluginAll() {
        List<Plugin> pluginAll = manager.getPluginAll();
        List<PluginVo> vos = new ArrayList<>();
        if (!CollectionUtils.isEmpty(pluginAll)) {
            Map<String, List<String>> pluginClassNameMap = new HashMap<>();
            List<Mapping> mappingAll = manager.getMappingAll();
            if (!CollectionUtils.isEmpty(mappingAll)) {
                mappingAll.forEach(mapping -> {
                    Plugin plugin = mapping.getPlugin();
                    if (null != plugin) {
                        pluginClassNameMap.putIfAbsent(plugin.getClassName(), new ArrayList<>());
                        pluginClassNameMap.get(plugin.getClassName()).add(mapping.getName());
                    }
                });
            }

            vos.addAll(pluginAll.stream().map(plugin -> {
                PluginVo vo = new PluginVo();
                BeanUtils.copyProperties(plugin, vo);
                vo.setMappingName(StringUtil.join(pluginClassNameMap.get(plugin.getClassName()), "|"));
                return vo;
            }).collect(Collectors.toList()));
        }
        return vos;
    }

    @Override
    public String getPluginPath() {
        return manager.getPluginPath();
    }

    @Override
    public String getLibraryPath() {
        return manager.getLibraryPath();
    }

    @Override
    public void loadPlugins() {
        manager.loadPlugins();
        logService.log(LogType.PluginLog.UPDATE);
    }

    @Override
    public void checkFileSuffix(String filename) {
        if (StringUtil.isNotBlank(filename)) {
            String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
            if (null == FileSuffixEnum.getFileSuffix(suffix)) {
                suffix = StringUtil.join(FileSuffixEnum.values(), ",").toLowerCase();
                String msg = String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。", filename, suffix);
                logService.log(LogType.PluginLog.CHECK_ERROR, msg);
                throw new BizException(msg);
            }
        }
    }
}