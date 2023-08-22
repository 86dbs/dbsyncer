package org.dbsyncer.parser;

import org.dbsyncer.parser.strategy.FlushStrategy;
import org.dbsyncer.parser.strategy.impl.DisableFullFlushStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 21:36
 */
@Configuration
public class ParserStrategyConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public FlushStrategy flushStrategy() {
        return new DisableFullFlushStrategy();
    }

}