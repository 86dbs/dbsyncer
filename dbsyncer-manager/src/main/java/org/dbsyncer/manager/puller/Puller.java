package org.dbsyncer.manager.puller;

import org.dbsyncer.parser.model.Mapping;
import org.springframework.scheduling.annotation.Async;

public interface Puller {

    @Async("taskExecutor")
    void asyncStart(Mapping mapping);

    void close(String metaId);

}