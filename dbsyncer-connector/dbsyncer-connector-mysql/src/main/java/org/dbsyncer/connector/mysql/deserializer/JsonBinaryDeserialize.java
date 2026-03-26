/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-08-30 00:22
 */
public final class JsonBinaryDeserialize {

    private static final String CHARSET_NAME = "UTF-8";

    public byte[] deserializeJson(int meta, ByteArrayInputStream inputStream) throws IOException {
        int blobLength = inputStream.readInteger(meta);
        byte[] bytes = inputStream.read(blobLength);
        JsonBinary jsonBinary = new JsonBinary(bytes);
        return jsonBinary.getString().getBytes(CHARSET_NAME);
    }
}
