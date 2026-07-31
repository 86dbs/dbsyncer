/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.flush;

/**
 * 缓冲请求。
 * <p>对明细分表写入场景，{@link #getMetaId()} 实际返回任务分片键（任务 ID），不是 Meta 主键。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 16:57
 */
public interface BufferRequest {

    /**
     * 分区键。明细分表场景为任务 ID（分片键）。
     *
     * @return 分区键
     */
    String getMetaId();
}
