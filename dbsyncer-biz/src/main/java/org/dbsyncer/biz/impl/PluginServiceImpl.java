package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.PluginVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.plugin.enums.FileSuffixEnum;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
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

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private LogService logService;

    @Override
    public List<PluginVo> getPluginAll() {
        List<Plugin> pluginAll = pluginFactory.getPluginAll();
        List<PluginVo> vos = new ArrayList<>();
        if (!CollectionUtils.isEmpty(pluginAll)) {
            Map<String, List<String>> pluginClassNameMap = getPluginClassNameMap();
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
        Map<String, List<String>> map = new HashMap<>();
        List<Mapping> mappingAll = profileComponent.getMappingAll();
        if (CollectionUtils.isEmpty(mappingAll)) {
            return map;
        }

        for (Mapping m : mappingAll) {
            Plugin plugin = m.getPlugin();
            if (null != plugin) {
                map.putIfAbsent(plugin.getClassName(), new ArrayList<>());
                map.get(plugin.getClassName()).add(m.getName());
                continue;
            }

            List<TableGroup> tableGroupAll = profileComponent.getTableGroupAll(m.getId());
            if (CollectionUtils.isEmpty(tableGroupAll)) {
                continue;
            }
            for (TableGroup t : tableGroupAll) {
                Plugin p = t.getPlugin();
                if (null != p) {
                    map.putIfAbsent(p.getClassName(), new ArrayList<>());
                    map.get(p.getClassName()).add(m.getName());
                    break;
                }
            }
        }

        return map;
    }
}