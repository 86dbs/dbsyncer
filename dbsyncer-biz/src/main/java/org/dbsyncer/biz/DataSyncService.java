/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.biz.vo.MessageVo;

import java.util.Map;

public interface DataSyncService {

    /**
     * 获取同步数据
     *
     * @param metaId
     * @param messageId
     * @return
     */
    MessageVo getMessageVo(String metaId, String messageId) throws Exception;

    /**
     * 获取Binlog
     *
     * @param row
     * @param prettyBytes
     * @return
     * @throws InvalidProtocolBufferException
     */
    Map getBinlogData(Map row, boolean prettyBytes) throws Exception;

    /**
     * 手动同步数据
     *
     * @param params
     * @return
     */
    String sync(Map<String, String> params) throws Exception;

    /**
     * 批量重试所有失败数据
     *
     * @param metaId 元数据ID
     * @return 包含总数、成功数、失败数的Map
     * @throws Exception
     */
    Map<String, Object> retryAll(String metaId) throws Exception;

}