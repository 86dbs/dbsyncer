/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import com.google.protobuf.ByteString;
import org.dbsyncer.biz.BinlogMessageService;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.springframework.stereotype.Component;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:07
 */
@Component
public class BinlogMessageServiceImpl implements BinlogMessageService {

    @Override
    public Object deserializeValue(ConnectorService connectorService, Field field, ByteString value) {
        // TODO 实现标准数据类型反序列化
        return BinlogMessageUtil.deserializeValue(field.getType(), value);
    }
}
