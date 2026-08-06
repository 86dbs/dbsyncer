/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Doris 二进制类型（BINARY / VARBINARY）。
 * <p>
 * Doris MySQL 协议 JDBC 读二进制列时，可打印内容常被驱动以 {@link String} 返回；
 * 校验/对比前需归一为 {@code byte[]}，与 MySQL BLOB/BINARY 标准域对齐。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-08-06
 */
public final class DorisBytesType extends BytesType {

    private enum TypeEnum {
        BINARY, VARBINARY;

        public String getValue() {
            return name();
        }
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(TypeEnum::getValue).collect(Collectors.toSet());
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof String) {
            // JDBC 将可打印二进制读成字符串时，按 Latin1 1:1 还原字节，避免 UTF-8 多字节破坏二进制。
            return ((String) val).getBytes(StandardCharsets.ISO_8859_1);
        }
        return throwUnsupportedException(val, field);
    }
}
