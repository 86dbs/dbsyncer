package org.dbsyncer.biz.checker.impl.config;

import org.apache.commons.lang3.StringUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.SHA1Util;
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
import org.springframework.beans.factory.annotation.Value;
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

    @Value(value = "${dbsyncer.web.login.password}")
    private String password;

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        Config config = new Config();
        config.setName("系统配置");
        config.setType(ConfigConstant.CONFIG);
        config.setPassword(password);

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

        // 修改密码
        String newPwd = params.get("newPwd");
        String oldPwd = params.get("oldPwd");
        if (StringUtil.isNotBlank(newPwd) && StringUtil.isNotBlank(oldPwd)) {
            oldPwd = SHA1Util.b64_sha1(oldPwd);
            if (!StringUtil.equals(config.getPassword(), oldPwd)) {
                logService.log(LogType.SystemLog.ERROR, "修改密码失败");
                throw new BizException("修改密码失败");
            }
            config.setPassword(SHA1Util.b64_sha1(newPwd));
            logService.log(LogType.SystemLog.INFO, "修改密码成功");
        }
        logService.log(LogType.SystemLog.INFO, "修改系统配置");

        // 刷新监控间隔（秒）
        String refreshInterval = params.get("refreshInterval");
        if (StringUtil.isNotBlank(refreshInterval)) {
            int time = NumberUtil.toInt(refreshInterval, 10);
            config.setRefreshInterval(time);
        }

        // 刷新邮箱配置
        String email = params.get("email");
        if (StringUtils.isNotBlank(email)){
            config.setEmail(email);
        }

        // 修改基本配置
        this.modifyConfigModel(config, params);
        return config;
    }

}