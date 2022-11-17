package org.dbsyncer.biz.checker.impl.config;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.Config;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class ConfigChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        Config config = new Config();
        config.setName("系统配置");
        config.setType(ConfigConstant.CONFIG);

        // 修改基本配置
        this.modifyConfigModel(config, params);

        manager.addConfig(config);
        return config;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "Config check params is null.");

        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Assert.hasText(id, "Config id is empty.");
        Config config = manager.getConfig(id);
        Assert.notNull(config, "配置文件为空.");

        // 刷新监控间隔（秒）
        String refreshInterval = params.get("refreshInterval");
        if (StringUtil.isNotBlank(refreshInterval)) {
            int time = NumberUtil.toInt(refreshInterval, 10);
            config.setRefreshInterval(time);
        }

        // 刷新邮箱配置(有配置则发邮件)
        String email = params.get("email");
        config.setEmail(email);
        logService.log(LogType.SystemLog.INFO, "修改系统配置");

        // 修改基本配置
        this.modifyConfigModel(config, params);
        return config;
    }

}