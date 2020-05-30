package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.checker.impl.config.ConfigChecker;
import org.dbsyncer.biz.vo.ConfigVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Config;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private ConfigChecker configChecker;

    @Override
    public String edit(Map<String, String> params) {
        synchronized (this) {
            Config config = manager.getConfig(ConfigConstant.CONFIG);
            if (null == config) {
                configChecker.checkAddConfigModel(params);
            }
            ConfigModel model = configChecker.checkEditConfigModel(params);
            manager.editConfig(model);
        }
        return "修改成功.";
    }

    @Override
    public ConfigVo getConfig() {
        List<Config> all = manager.getConfigAll();
        Config config = CollectionUtils.isEmpty(all) ? (Config) configChecker.checkAddConfigModel(new HashMap<>()) : all.get(0);
        return convertConfig2Vo(config);
    }

    @Override
    public String getPassword() {
        List<Config> all = manager.getConfigAll();
        Config config = CollectionUtils.isEmpty(all) ? (Config) configChecker.checkAddConfigModel(new HashMap<>()) : all.get(0);
        return config.getPassword();
    }

    @Override
    public List<ConfigVo> queryConfig() {
        List<ConfigVo> list = manager.getConfigAll().stream()
                .map(config -> convertConfig2Vo(config))
                .collect(Collectors.toList());
        return list;
    }

    private ConfigVo convertConfig2Vo(Config config) {
        ConfigVo configVo = new ConfigVo();
        BeanUtils.copyProperties(config, configVo);
        // 避免密码直接暴露
        configVo.setPassword("");
        return configVo;
    }

}