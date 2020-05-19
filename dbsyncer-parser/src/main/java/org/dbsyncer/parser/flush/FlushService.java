package org.dbsyncer.parser.flush;

import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;

public interface FlushService {

    /**
     * 记录错误日志
     *
     * @param metaId
     * @param error
     */
    @Async("taskExecutor")
    void asyncWrite(String metaId, String error);

    /**
     * 记录数据
     *
     * @param metaId
     * @param success
     * @param data
     */
    @Async("taskExecutor")
    void asyncWrite(String metaId, boolean success, List<Map<String, Object>> data);
}