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
    MessageVo getMessageVo(String metaId, String messageId);

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