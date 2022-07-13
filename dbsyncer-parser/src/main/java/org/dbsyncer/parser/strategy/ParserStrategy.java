package org.dbsyncer.parser.strategy;

import java.util.List;
import java.util.Map;

public interface ParserStrategy {

    void execute(String tableGroupId, String event, Map<String, Object> data);

    default void complete(List<String> messageIds) {}
}