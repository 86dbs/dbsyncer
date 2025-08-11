package org.dbsyncer.manager;

import org.dbsyncer.parser.model.Mapping;

public interface Puller {

    void start(Mapping mapping);

    void close(String metaId);

}