package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.XmlType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle XML类型支持（使用XMLTYPE存储）
 */
public final class OracleXmlType extends XmlType {

    private enum TypeEnum {
        XMLTYPE
    }

    @Override
    public Set<String> getSupportedTypeName() {
        return Arrays.stream(TypeEnum.values()).map(Enum::name).collect(Collectors.toSet());
    }

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof String) {
            return (String) val;
        }
        // 由于缺少Oracle XML相关的依赖，暂时只支持String类型
        return throwUnsupportedException(val, field);
    }
}