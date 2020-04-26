package org.dbsyncer.manager.extractor;

import org.dbsyncer.parser.model.Mapping;

public interface Extractor {

    void start(Mapping mapping);

    void close(String metaId);

}
