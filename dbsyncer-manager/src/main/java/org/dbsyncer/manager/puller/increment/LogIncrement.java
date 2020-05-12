package org.dbsyncer.manager.puller.increment;

import org.dbsyncer.listener.Extractor;
import org.dbsyncer.manager.puller.AbstractIncrement;
import org.springframework.stereotype.Component;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-08 00:31
 */
@Component
public class LogIncrement extends AbstractIncrement {

    @Override
    public void execute(String mappingId, String metaId, Extractor extractor) {
        extractor.extract();
    }
}
