/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.config;

import org.dbsyncer.web.security.AESKeyManager;
import org.dbsyncer.web.security.TimestampValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * 安全配置类
 * 初始化安全相关的缓存和定时任务
 * 
 * @author 穿云
 * @version 1.0.0
 */
@Configuration
public class SecurityConfig {

    private static final Logger logger = LoggerFactory.getLogger(SecurityConfig.class);

    /**
     * 初始化安全相关的缓存和定时任务
     */
    @PostConstruct
    public void init() {
        logger.info("初始化安全配置...");
        AESKeyManager.init();
        TimestampValidator.init();
        logger.info("安全配置初始化完成");
    }

    /**
     * 清理资源
     */
    @PreDestroy
    public void destroy() {
        logger.info("清理安全配置资源...");
        // 注意：ExpiringCache 的 destroy 方法会在 JVM 关闭钩子中自动调用
        logger.info("安全配置资源清理完成");
    }
}
