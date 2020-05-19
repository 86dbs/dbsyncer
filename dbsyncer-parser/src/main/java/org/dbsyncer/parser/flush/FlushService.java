package org.dbsyncer.parser.flush;

import org.springframework.scheduling.annotation.Async;

import java.util.Map;
import java.util.Queue;

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
     * 记录错误数据
     *
     * @param metaId
     * @param failData
     */
    @Async("taskExecutor")
    void asyncWrite(String metaId, Queue<Map<String,Object>> failData);

}