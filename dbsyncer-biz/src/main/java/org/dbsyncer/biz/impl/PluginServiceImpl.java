package org.dbsyncer.biz.impl;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
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

    @Autowired
    private LogService logService;

    @Override
    public List<Plugin> getPluginAll() {
        return manager.getPluginAll();
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
        if (StringUtils.isNotBlank(filename)) {
            String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
            if (null == FileSuffixEnum.getFileSuffix(suffix)) {
                suffix = StringUtils.join(FileSuffixEnum.values(), ",").toLowerCase();
                String msg = String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。", filename, suffix);
                logService.log(LogType.PluginLog.CHECK_ERROR, msg);
                throw new BizException(msg);
            }
        }
    }
}