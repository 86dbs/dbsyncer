package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/30 0:22
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
