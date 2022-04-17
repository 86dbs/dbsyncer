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

    TEST_DECODING("test_decoding", TestDecodingMessageDecoder.class),

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
        throw new ListenerException(String.format("MessageDecoder type \"%s\" does not exist.", type));
    }

    public String getType() {
        return type;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}
