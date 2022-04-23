package org.dbsyncer.listener.postgresql.decoder;

import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.postgresql.AbstractMessageDecoder;
import org.dbsyncer.listener.postgresql.column.ColumnValueResolver;
import org.dbsyncer.listener.postgresql.column.TestDecodingColumnValue;
import org.dbsyncer.listener.postgresql.enums.MessageDecoderEnum;
import org.dbsyncer.listener.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.LogSequenceNumber;
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

    private static final ColumnValueResolver resolver = new ColumnValueResolver();

    @Override
    public boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn) {
        if (super.skipMessage(buffer, startLsn, lastReceiveLsn)) {
            return true;
        }
        int position = buffer.position();
        try {
            MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
            switch (type) {
                case BEGIN:
                case COMMIT:
                case RELATION:
                case TRUNCATE:
                case TYPE:
                case ORIGIN:
                case NONE:
                    return true;
                default:
                    // TABLE|INSERT|UPDATE|DELETE
                    return false;
            }
        } finally {
            buffer.position(position);
        }
    }

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
        logger.info(message);
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
            data.add(resolver.resolveValue(type, new TestDecodingColumnValue(value)));
        }

        RowChangedEvent event = null;
        if (StringUtil.equals(ConnectorConstant.OPERTION_UPDATE, eventType)) {
            event = new RowChangedEvent(table, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, data);
        }

        if (StringUtil.equals(ConnectorConstant.OPERTION_INSERT, eventType)) {
            event = new RowChangedEvent(table, ConnectorConstant.OPERTION_INSERT, Collections.EMPTY_LIST, data);
        }

        if (StringUtil.equals(ConnectorConstant.OPERTION_DELETE, eventType)) {
            event = new RowChangedEvent(table, ConnectorConstant.OPERTION_DELETE, data, Collections.EMPTY_LIST);
        }
        logger.info(event.toString());

        return null;
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

    final class Lexer {
        private final String input;
        private final char[] array;
        private final int length;
        private int pos = 0;
        private String token;

        public Lexer(String input) {
            this.input = input;
            this.array = input.toCharArray();
            this.length = this.array.length;
        }

        public String token() {
            return token;
        }

        public String nextToken(char comma) {
            if (pos < length) {
                StringBuilder out = new StringBuilder(16);
                while (pos < length && array[pos] != comma) {
                    out.append(array[pos]);
                    pos++;
                }
                pos++;
                return token = out.toString();
            }
            return token = null;
        }

        public String nextTokenToQuote() {
            if (pos < length) {
                int commaCount = 1;
                StringBuilder out = new StringBuilder(16);
                while (!((pos == length - 1 || (array[pos + 1] == ' ' && commaCount % 2 == 1)) && array[pos] == '\'')) {
                    if (array[pos] == '\'') {
                        commaCount++;
                    }
                    out.append(array[pos]);
                    pos++;
                }
                pos++;
                return token = out.toString();
            }
            return token = null;
        }

        public void skip(int skip) {
            this.pos += skip;
        }

        public char current() {
            return array[pos];
        }

        public boolean hasNext() {
            return pos < length;
        }
    }

}