package org.dbsyncer.listener.postgresql.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.postgresql.MessageDecoder;
import org.dbsyncer.listener.postgresql.decoder.PgOutputMessageDecoder;
import org.dbsyncer.listener.postgresql.decoder.TestDecodingMessageDecoder;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:05
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

    public static MessageDecoder getMessageDecoder(String type) throws ListenerException, IllegalAccessException, InstantiationException {
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
