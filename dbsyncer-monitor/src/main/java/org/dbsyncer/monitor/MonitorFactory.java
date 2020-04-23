package org.dbsyncer.monitor;

import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/23 11:30
 */
@Component
public class MonitorFactory implements Monitor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Manager manager;

    @Override
    @Cacheable(value = "connector", keyGenerator = "cacheKeyGenerator")
    public boolean alive(String id) {
        logger.info("{}从DB检查alive,id:{}", LocalDateTime.now(), id);
        Connector connector = manager.getConnector(id);
        return null != connector ? manager.alive(connector.getConfig()) : false;
    }

}