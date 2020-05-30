package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConfigService;
import org.dbsyncer.biz.checker.impl.config.ConfigChecker;
import org.dbsyncer.biz.vo.ConfigVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Config;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class ConfigServiceImpl extends BaseServiceImpl implements ConfigService {

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
            log(LogType.SystemLog.INFO, model);
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

    private ConfigVo convertConfig2Vo(Config config) {
        ConfigVo configVo = new ConfigVo();
        configVo.setId(config.getId());
        return configVo;
    }

}