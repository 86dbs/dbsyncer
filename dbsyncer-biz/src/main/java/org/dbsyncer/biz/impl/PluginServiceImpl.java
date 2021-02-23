package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.plugin.config.Plugin;
import org.dbsyncer.plugin.enums.FileSuffixEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
@Component
public class PluginServiceImpl implements PluginService {

    @Autowired
    private Manager manager;

    @Override
    public List<Plugin> getPluginAll() {
        return manager.getPluginAll();
    }

    @Override
    public String getPluginPath() {
        return manager.getPluginPath();
    }

    @Override
    public void loadPlugins() {
        manager.loadPlugins();
    }

    @Override
    public void checkFileSuffix(String filename) {
        if (StringUtils.isNotBlank(filename)) {
            String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
            // 暂支持jar文件
            if(!FileSuffixEnum.isJar(suffix)){
                throw new BizException(String.format("不正确的文件扩展名 \"%s\". 只支持 \"jar\" 的文件扩展名.", filename));
            }
        }
    }
}