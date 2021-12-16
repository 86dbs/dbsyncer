package org.dbsyncer.connector.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;

/**
 * Kafka序列化类型
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/08 21:13
 */
public enum KafkaSerializerTypeEnum {

    JSON("json"),
    JAVABEAN("javabean");

    private String code;

    KafkaSerializerTypeEnum(String code) {
        this.code = code;
    }

    public static KafkaSerializerTypeEnum getSerializerType(String code) throws ConnectorException {
        for (KafkaSerializerTypeEnum e : KafkaSerializerTypeEnum.values()) {
            if (StringUtil.equals(e.getCode(), code)) {
                return e;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }

}