package org.dbsyncer.connector.oracle.schema.support;

import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.support.XmlType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Oracle XML类型支持（使用XMLTYPE存储）
 */
public final class OracleXmlType extends XmlType {

    @Override
    public Set<String> getSupportedTypeName() {
        return new HashSet<>(Arrays.asList("XMLTYPE"));
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