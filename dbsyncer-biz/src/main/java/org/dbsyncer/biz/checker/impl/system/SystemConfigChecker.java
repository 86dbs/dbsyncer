package org.dbsyncer.biz.checker.impl.system;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.SystemConfig;
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
public class SystemConfigChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Autowired
    private LogService logService;

    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.setName("系统配置");

        // 修改基本配置
        this.modifyConfigModel(systemConfig, params);

        manager.addConfigModel(systemConfig);
        return systemConfig;
    }

    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        logger.info("params:{}", params);
        Assert.notEmpty(params, "Config check params is null.");

        SystemConfig systemConfig = manager.getSystemConfig();
        Assert.notNull(systemConfig, "配置文件为空.");

        // 刷新监控间隔（秒）
        String refreshInterval = params.get("refreshInterval");
        if (StringUtil.isNotBlank(refreshInterval)) {
            int time = NumberUtil.toInt(refreshInterval, 10);
            systemConfig.setRefreshInterval(time);
        }
        logService.log(LogType.SystemLog.INFO, "修改系统配置");

        // 修改基本配置
        this.modifyConfigModel(systemConfig, params);
        return systemConfig;
    }

}