package org.dbsyncer.manager.extractor;

import org.dbsyncer.parser.model.Mapping;
import org.springframework.scheduling.annotation.Async;

public interface Extractor {

    @Async("taskExecutor")
    void asyncStart(Mapping mapping);

    void close(String metaId);

}