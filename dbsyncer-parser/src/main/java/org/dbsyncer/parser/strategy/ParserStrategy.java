package org.dbsyncer.parser.strategy;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;

public interface ParserStrategy {

    void execute(Mapping mapping, TableGroup tableGroup, RowChangedEvent event);
}
