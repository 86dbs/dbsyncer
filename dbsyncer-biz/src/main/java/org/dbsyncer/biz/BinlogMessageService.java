/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import com.google.protobuf.ByteString;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.spi.ConnectorService;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:06
 */
public interface BinlogMessageService {

    /**
     * 反序列化
     *
     * @param connectorService 目标连接器服务
     * @param field 目标字段
     * @param value 目标值
     */
    Object deserializeValue(ConnectorService connectorService, Field field, ByteString value);

}
