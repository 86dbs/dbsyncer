package org.dbsyncer.manager.extractor;

import org.dbsyncer.listener.Listener;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 增量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class IncrementExtractor extends AbstractExtractor {

    @Autowired
    private Listener listener;

    @Override
    public void asyncStart(Mapping mapping) {

    }

    @Override
    public void close(String metaId) {

    }
}