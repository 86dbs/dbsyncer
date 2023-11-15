package org.dbsyncer.biz.impl;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.SystemConfigVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.plugin.enums.FileSuffixEnum;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
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

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private Checker systemConfigChecker;

    @Resource
    private LogService logService;

    @Resource
    private UserConfigService userConfigService;

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = systemConfigChecker.checkEditConfigModel(params);
        profileComponent.editConfigModel(model);
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
        list.add(userConfigService.getUserConfig());
        profileComponent.getConnectorAll().forEach(config -> list.add(config));
        profileComponent.getMappingAll().forEach(config -> list.add(config));
        profileComponent.getMetaAll().forEach(config -> list.add(config));
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

    @Override
    public boolean isEnableCDN() {
        return getSystemConfig().isEnableCDN();
    }

    private SystemConfig getSystemConfig() {
        SystemConfig config = profileComponent.getSystemConfig();
        if (null != config) {
            return config;
        }

        synchronized (this) {
            config = profileComponent.getSystemConfig();
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