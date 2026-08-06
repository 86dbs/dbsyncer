/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.BytesType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 达梦二进制类型（含 MySQL BLOB 族别名；BLOB/RAW 也可由 Oracle 映射覆盖）
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-07-18
 */
public final class DamengBytesType extends BytesType {

    private enum TypeEnum {
        BINARY, VARBINARY, IMAGE, LONGVARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected byte[] getDefaultMergedVal(Field field) {
        return new byte[0];
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        if (val instanceof String) {
            return ((String) val).getBytes(StandardCharsets.UTF_8);
        }
        return throwUnsupportedException(val, field);
    }
}
