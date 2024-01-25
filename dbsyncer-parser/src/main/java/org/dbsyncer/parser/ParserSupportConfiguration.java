/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.flush.impl.TableGroupBufferActuator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-01-25 23:43
 */
@Configuration
public class ParserSupportConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TableGroupBufferActuator tableGroupBufferActuator() {
        return new TableGroupBufferActuator();
    }
}