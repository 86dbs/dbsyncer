package org.dbsyncer.parser.flush;

import java.util.List;
import java.util.Map;

public interface FlushService {

    /**
     * 记录错误日志
     *
     * @param type
     * @param error
     */
    void asyncWrite(String type, String error);

    /**
     * 记录数据
     *
     * @param metaId
     * @param tableGroupId
     * @param targetTableGroupName
     * @param event
     * @param success
     * @param data
     */
    void asyncWrite(String metaId, String tableGroupId, String targetTableGroupName, String event, boolean success, List<Map> data, String error);
}