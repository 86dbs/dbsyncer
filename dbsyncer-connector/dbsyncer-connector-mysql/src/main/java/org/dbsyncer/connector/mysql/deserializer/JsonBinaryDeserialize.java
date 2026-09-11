/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.deserializer;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-08-30 00:22
 */
public final class JsonBinaryDeserialize {

    private static final Logger logger = LoggerFactory.getLogger(JsonBinaryDeserialize.class);

    private static final byte[] EMPTY_JSON_BYTES = "".getBytes(StandardCharsets.UTF_8);

    /**
     * 读取并解析 JSON 列的 binary 内容为 UTF-8 文本。
     *
     * @param meta        长度字段占用字节数
     * @param inputStream binlog 事件流
     * @return JSON 文本的 UTF-8 字节；空内容或解析失败时返回空串
     * @throws IOException 读取 binlog 流失败时抛出
     */
    public byte[] deserializeJson(int meta, ByteArrayInputStream inputStream) throws IOException {
        int blobLength = inputStream.readInteger(meta);
        byte[] bytes = inputStream.read(blobLength);
        if (bytes == null || bytes.length == 0) {
            logger.warn("JSON binary is empty, fallback to empty string");
            return EMPTY_JSON_BYTES;
        }
        try {
            return new JsonBinary(bytes).getString().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            // JsonBinary.getString 可能将 IOException 包成 RuntimeException（如 EOFException）
            logger.warn("Failed to parse JSON binary (len={}), fallback to empty string: {}",
                    bytes.length, e.toString());
            return EMPTY_JSON_BYTES;
        }
    }
}
