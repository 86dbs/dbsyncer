package org.dbsyncer.listener.mysql.io;

import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;

import java.io.IOException;

public interface XOutputStream {

    void flush() throws IOException;

    void close() throws IOException;

    void writeBytes(byte value[]) throws IOException;

    void writeBytes(int value, int length) throws IOException;

    void writeInt(int value, int length) throws IOException;

    void writeLong(long value, int length) throws IOException;

    void writeUnsignedLong(UnsignedLong value) throws IOException;

    void writeLengthCodedString(StringColumn value) throws IOException;

    void writeFixedLengthString(StringColumn value) throws IOException;

    void writeNullTerminatedString(StringColumn value) throws IOException;

    void writeBytes(byte value[], int offset, int length) throws IOException;
}
