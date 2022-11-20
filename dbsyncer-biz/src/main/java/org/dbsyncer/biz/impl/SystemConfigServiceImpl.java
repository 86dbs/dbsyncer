package org.dbsyncer.biz.impl;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.UserService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.SystemConfigVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.template.PreloadTemplate;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.plugin.enums.FileSuffixEnum;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class SystemConfigServiceImpl implements SystemConfigService {

    @Autowired
    private Manager manager;

    @Autowired
    private Checker systemConfigChecker;

    @Autowired
    private PreloadTemplate preloadTemplate;

    @Autowired
    private LogService logService;

    @Autowired
    private UserService userService;

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = systemConfigChecker.checkEditConfigModel(params);
        manager.editConfigModel(model);
        return "修改成功.";
    }

    @Override
    public SystemConfigVo getSystemConfigVo() {
        return convertConfig2Vo(getSystemConfig());
    }

    @Override
    public List<ConfigModel> getConfigModelAll() {
        List<ConfigModel> list = new ArrayList<>();
        list.add(getSystemConfig());
        list.add(userService.getUserConfig());
        manager.getConnectorAll().forEach(config -> list.add(config));
        manager.getMappingAll().forEach(config -> list.add(config));
        manager.getMetaAll().forEach(config -> list.add(config));
        return list;
    }

    @Override
    public void checkFileSuffix(String filename) {
        Assert.hasText(filename, "the config filename is null.");
        String suffix = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
        FileSuffixEnum fileSuffix = FileSuffixEnum.getFileSuffix(suffix);
        Assert.notNull(fileSuffix, "Illegal file suffix");
        Assert.isTrue(FileSuffixEnum.JSON == fileSuffix, String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。", filename, FileSuffixEnum.JSON.getName()));
    }

    @Override
    public void refreshConfig(File file) {
        Assert.notNull(file, "the config file is null.");
        try {
            List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
            if (!CollectionUtils.isEmpty(lines)) {
                StringBuilder json = new StringBuilder();
                lines.forEach(line -> json.append(line));
                preloadTemplate.reload(json.toString());
            }
        } catch (IOException e) {
            logService.log(LogType.CacheLog.IMPORT_ERROR);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private SystemConfig getSystemConfig() {
        SystemConfig config = manager.getSystemConfig();
        if (null != config) {
            return config;
        }

        synchronized (this) {
            config = manager.getSystemConfig();
            if (null == config) {
                config = (SystemConfig) systemConfigChecker.checkAddConfigModel(new HashMap<>());
            }
            return config;
        }
    }

    private SystemConfigVo convertConfig2Vo(SystemConfig systemConfig) {
        SystemConfigVo systemConfigVo = new SystemConfigVo();
        BeanUtils.copyProperties(systemConfig, systemConfigVo);
        return systemConfigVo;
    }

}