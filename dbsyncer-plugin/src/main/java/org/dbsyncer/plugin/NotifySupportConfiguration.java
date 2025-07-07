package org.dbsyncer.plugin;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/13 22:20
 */
@Configuration
public class NotifySupportConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public NotifyService notifyService() {
        return notifyMessage -> {};
    }

}