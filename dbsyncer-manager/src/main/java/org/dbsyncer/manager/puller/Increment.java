package org.dbsyncer.manager.puller;

import org.dbsyncer.listener.Extractor;

public interface Increment {

    void execute(Extractor extractor);

}