package org.dbsyncer.manager;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;

public interface Puller {

    void start(Mapping mapping);

    void close(Mapping mapping);
}