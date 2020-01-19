package org.dbsyncer.connector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/19 23:17
 */
@Configuration
public class AutoConfiguration {

    @Bean
    public ConnectorFactory connectorFactory() {
        return new ConnectorFactory();
    }

}
