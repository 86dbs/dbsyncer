/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.postgresql.decoder.MessageDecoder;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.connector.postgresql.decoder.impl.PgOutputMessageDecoder;
import org.dbsyncer.connector.postgresql.decoder.impl.TestDecodingMessageDecoder;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-10 22:36
 */
public enum MessageDecoderEnum {

    /**
     * 插件：TEST_DECODING
     */
    TEST_DECODING("test_decoding", TestDecodingMessageDecoder.class),

    /**
     * 插件：PG_OUTPUT
     */
    PG_OUTPUT("pgoutput", PgOutputMessageDecoder.class);

    private String type;
    private Class<?> clazz;

    MessageDecoderEnum(String type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    public static MessageDecoder getMessageDecoder(String type) throws PostgreSQLException, IllegalAccessException, InstantiationException {
        for (MessageDecoderEnum e : MessageDecoderEnum.values()) {
            if (StringUtil.equals(type, e.getType())) {
                return (MessageDecoder) e.getClazz().newInstance();
            }
        }
        return (MessageDecoder) TEST_DECODING.getClazz().newInstance();
    }

    public String getType() {
        return type;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
