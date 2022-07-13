package org.dbsyncer.parser.strategy;

import java.util.List;
import java.util.Map;

public interface ParserStrategy {

    /**
     * 同步消息
     *
     * @param tableGroupId
     * @param event
     * @param data
     */
    void execute(String tableGroupId, String event, Map<String, Object> data);

    /**
     * 完成同步后，执行回调
     *
     * @param messageIds
     */
    default void complete(List<String> messageIds) {
    }
}