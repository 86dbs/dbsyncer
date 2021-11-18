package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.strategy.AbstractFlushStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 记录全量和增量同步数据
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/11/18 22:21
 */
@Component
@ConditionalOnProperty(prefix = "dbsyncer.parser.flush", value = "enabled", havingValue = "true")
public final class EnableFlushStrategy extends AbstractFlushStrategy {

}