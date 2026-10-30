/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.PluginVO;
import org.dbsyncer.common.enums.FileSuffixEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.sdk.model.Plugin;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
@Service
public class PluginServiceImpl implements PluginService {

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private LogService logService;

    @Override
    public List<PluginVO> listPlugins() {
        List<Plugin> pluginAll = pluginFactory.getPluginAll();
        if (CollectionUtils.isEmpty(pluginAll)) {
            return new ArrayList<>();
        }
        return pluginAll.stream().map(this::toPluginVO).collect(Collectors.toList());
    }

    private PluginVO toPluginVO(Plugin plugin) {
        PluginVO vo = new PluginVO();
        BeanUtils.copyProperties(plugin, vo);
        return vo;
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

}