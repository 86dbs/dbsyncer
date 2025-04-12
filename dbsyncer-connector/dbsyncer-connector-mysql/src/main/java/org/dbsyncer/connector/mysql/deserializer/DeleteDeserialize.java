/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.DeleteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-04-12 15:21
 */
public class DeleteDeserialize extends DeleteRowsEventDataDeserializer {
    private final JsonBinaryDeserialize jsonBinaryDeserialize = new JsonBinaryDeserialize();

    public DeleteDeserialize(Map<Long, TableMapEventData> tableMapEventByTableId) {
        super(tableMapEventByTableId);
    }

    protected byte[] deserializeJson(int meta, ByteArrayInputStream inputStream) throws IOException {
        return jsonBinaryDeserialize.deserializeJson(meta, inputStream);
    }
}