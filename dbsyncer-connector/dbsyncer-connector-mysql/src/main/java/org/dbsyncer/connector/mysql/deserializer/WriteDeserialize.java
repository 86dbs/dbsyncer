/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-04-12 15:21
 */
public class WriteDeserialize extends WriteRowsEventDataDeserializer {
    private final JsonBinaryDeserialize jsonBinaryDeserialize = new JsonBinaryDeserialize();

    public WriteDeserialize(Map<Long, TableMapEventData> tableMapEventByTableId) {
        super(tableMapEventByTableId);
    }

    protected byte[] deserializeJson(int meta, ByteArrayInputStream inputStream) throws IOException {
        return jsonBinaryDeserialize.deserializeJson(meta, inputStream);
    }
}