/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-08-30 00:22
 */
public final class JsonBinaryDeserialize {

    private static final Logger logger = LoggerFactory.getLogger(JsonBinaryDeserialize.class);
    private static final String CHARSET_NAME = "UTF-8";

    public byte[] deserializeJson(int meta, ByteArrayInputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("Input stream is null");
        }

        int blobLength = inputStream.readInteger(meta);
        // 如果 blobLength <= 0，说明数据异常或空 JSON
        // 注意：MySQL binlog 中，NULL 值通过 NULL bitmap 标识，不会调用此方法
        // 如果调用了此方法但 blobLength <= 0，可能是：
        // 1. 空 JSON（如 JSON_OBJECT() 或 JSON_ARRAY() 但为空）- 返回空数组，让上层处理
        // 2. 数据异常（blobLength < 0）- 记录警告并返回空数组
        if (blobLength < 0) {
            logger.error("Invalid blobLength={} for JSON field (meta={}). This may indicate data corruption. Returning empty array.", 
                    blobLength, meta);
            throw new IOException("Invalid blobLength for JSON field");
        }
        if (blobLength == 0) {
            return "null".getBytes(CHARSET_NAME);
        }

        // 检查是否有足够的数据可读
        int available = inputStream.available();
        if (available < blobLength) {
            logger.error("Insufficient data for JSON field: meta={}, blobLength={}, available={}",
                    meta, blobLength, available);
            throw new IOException("Insufficient data for JSON field");
        }

        byte[] bytes = inputStream.read(blobLength);


        // 检查读取的数据长度是否匹配
        if (bytes == null || bytes.length != blobLength) {
            logger.error("Mismatched JSON data length: meta={}, expected={}, actual={}",
                    meta, blobLength, bytes.length);
            throw new IOException("Mismatched JSON data length");
        }

        JsonBinary jsonBinary = new JsonBinary(bytes);
        return jsonBinary.getString().getBytes(CHARSET_NAME);
    }
}
