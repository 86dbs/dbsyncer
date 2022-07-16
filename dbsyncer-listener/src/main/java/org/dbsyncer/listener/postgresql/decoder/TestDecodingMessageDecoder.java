package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.common.column.Lexer;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/4/17 23:00
 */
public class TestDecodingMessageDecoder extends AbstractMessageDecoder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public RowChangedEvent processMessage(ByteBuffer buffer) {
        if (!buffer.hasArray()) {
            throw new IllegalStateException("Invalid buffer received from PG server during streaming replication");
        }
        MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
        if (MessageTypeEnum.TABLE == type) {
            int offset = buffer.arrayOffset();
            byte[] source = buffer.array();
            return parseMessage(new String(source, offset, (source.length - offset)));
        }
        return null;
    }

    @Override
    public String getOutputPlugin() {
        return MessageDecoderEnum.TEST_DECODING.getType();
    }

    @Override
    public void withSlotOption(ChainedLogicalStreamBuilder builder) {
        builder.withSlotOption("include-xids", true)
                .withSlotOption("skip-empty-xacts", true);
    }

    private RowChangedEvent parseMessage(String message) {
        Lexer lexer = new Lexer(message);

        // table
        lexer.nextToken(' ');
        // schemaName
        lexer.nextToken('.');
        // tableName
        lexer.skip(1);
        String table = lexer.nextToken('"');
        lexer.skip(2);
        // eventType
        String eventType = lexer.nextToken(':');
        lexer.skip(1);

        List<Object> data = new ArrayList<>();
        while (lexer.hasNext()) {
            String name = parseName(lexer);
            if ("(no-tuple-data)".equals(name)) {
                // 删除时,无主键,不能同步
                return null;
            }
            String type = parseType(lexer);
            lexer.skip(1);
            String value = parseValue(lexer);
            data.add(resolveValue(type, value));
        }

        RowChangedEvent event = null;
        switch (eventType) {
            case ConnectorConstant.OPERTION_UPDATE:
            case ConnectorConstant.OPERTION_INSERT:
            case ConnectorConstant.OPERTION_DELETE:
                event = new RowChangedEvent(table, eventType, data);
                break;

            default:
                logger.info("Type {} not implemented", eventType);
        }
        return event;
    }

    private String parseName(Lexer lexer) {
        if (lexer.current() == ' ') {
            lexer.skip(1);
        }
        lexer.nextToken('[');
        return lexer.token();
    }

    private String parseType(Lexer lexer) {
        lexer.nextToken(']');
        return lexer.token();
    }

    private String parseValue(Lexer lexer) {
        if (lexer.current() == '\'') {
            lexer.skip(1);
            lexer.nextTokenToQuote();
            return lexer.token();
        }
        lexer.nextToken(' ');
        return lexer.token();
    }

}