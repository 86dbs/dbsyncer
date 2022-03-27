package org.dbsyncer.parser.config;

import org.dbsyncer.parser.flush.FlushStrategy;
import org.dbsyncer.parser.flush.impl.DisableFullFlushStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:36
 */
@Configuration
public class ParserFlushStrategyConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public FlushStrategy flushStrategy() {
        return new DisableFullFlushStrategy();
    }

}