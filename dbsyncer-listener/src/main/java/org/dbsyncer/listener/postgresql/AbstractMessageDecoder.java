package org.dbsyncer.listener.postgresql;

import org.dbsyncer.connector.config.DatabaseConfig;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:04
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    protected MessageDecoderContext messageDecoderContext;

    @Override
    public String getSlotName() {
        DatabaseConfig config = messageDecoderContext.getConfig();
        return String.format("DBSyncer-%s-%s", config.getSchema(), config.getUsername());
    }

    public void setMessageDecoderContext(MessageDecoderContext messageDecoderContext) {
        this.messageDecoderContext = messageDecoderContext;
    }
}
