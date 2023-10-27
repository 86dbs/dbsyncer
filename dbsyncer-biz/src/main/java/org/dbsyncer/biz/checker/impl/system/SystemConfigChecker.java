package org.dbsyncer.biz.checker.impl.system;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.BeanUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogService;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class SystemConfigChecker extends AbstractChecker {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private Manager manager;

    @Resource
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
        params.put("enableCDN", StringUtil.isNotBlank(params.get("enableCDN")) ? "true" : "false");

        SystemConfig systemConfig = manager.getSystemConfig();
        Assert.notNull(systemConfig, "配置文件为空.");
        BeanUtil.mapToBean(params, systemConfig);
        logService.log(LogType.SystemLog.INFO, "修改系统配置");

        // 修改基本配置
        this.modifyConfigModel(systemConfig, params);
        return systemConfig;
    }

}