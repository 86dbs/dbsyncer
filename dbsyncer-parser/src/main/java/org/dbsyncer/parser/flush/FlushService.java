package org.dbsyncer.parser.flush;

import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;

public interface FlushService {

    /**
     * 记录错误日志
     *
     * @param type
     * @param error
     */
    @Async("taskExecutor")
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
    void write(String metaId, String tableGroupId, String targetTableGroupName, String event, boolean success, List<Map> data, String error);
}