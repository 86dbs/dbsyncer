package org.dbsyncer.biz.impl;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.ConfigVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.manager.template.PreloadTemplate;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Config;
import org.dbsyncer.parser.model.ConfigModel;
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
public class ConfigServiceImpl implements ConfigService {

    @Autowired
    private Manager manager;

    @Autowired
    private Checker configChecker;

    @Autowired
    private PreloadTemplate preloadTemplate;

    @Autowired
    private LogService logService;

    @Override
    public synchronized String edit(Map<String, String> params) {
        getConfigModel();
        ConfigModel model = configChecker.checkEditConfigModel(params);
        manager.editConfig(model);
        return "修改成功.";
    }

    @Override
    public ConfigVo getConfig() {
        return convertConfig2Vo(getConfigModel());
    }

    @Override
    public List<ConfigModel> getConfigModelAll() {
        List<ConfigModel> list = new ArrayList<>();
        list.add(getConfig());
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

    private synchronized Config getConfigModel() {
        List<Config> all = manager.getConfigAll();
        return CollectionUtils.isEmpty(all) ? (Config) configChecker.checkAddConfigModel(new HashMap<>()) : all.get(0);
    }

    private ConfigVo convertConfig2Vo(Config config) {
        ConfigVo configVo = new ConfigVo();
        BeanUtils.copyProperties(config, configVo);
        return configVo;
    }

}