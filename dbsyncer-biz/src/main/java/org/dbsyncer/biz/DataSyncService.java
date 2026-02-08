/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.MessageVO;

import com.google.protobuf.InvalidProtocolBufferException;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Map;

public interface DataSyncService {

    /**
     * 获取同步数据
     *
     * @param metaId
     * @param messageId
     * @return
     */
    MessageVO getMessageVo(String metaId, String messageId);

    /**
     * 获取Binlog
     *
     * @param row
     * @param prettyBytes
     * @return
     * @throws InvalidProtocolBufferException
     */
    Map getBinlogData(Map row, boolean prettyBytes) throws InvalidProtocolBufferException;

    /**
     * 手动同步数据
     *
     * @param params
     * @return
     */
    String sync(Map<String, String> params) throws InvalidProtocolBufferException;

}