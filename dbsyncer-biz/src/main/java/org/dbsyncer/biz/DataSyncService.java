/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbsyncer.biz.model.DataSyncRequest;
import org.dbsyncer.biz.vo.MessageVO;

import java.util.Map;

public interface DataSyncService {

    /**
     * 获取同步数据
     */
    MessageVO getMessageVo(String metaId, String messageId);

    /**
     * 获取Binlog
     */
    Map getBinlogData(Map row, boolean prettyBytes) throws InvalidProtocolBufferException;

    /**
     * 手动同步数据
     */
    String sync(Map<String, String> params) throws InvalidProtocolBufferException;

    /**
     * 批量同步数据
     */
    void syncBatch(DataSyncRequest request);

}