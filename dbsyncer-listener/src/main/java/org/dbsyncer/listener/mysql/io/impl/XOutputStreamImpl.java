package org.dbsyncer.listener.mysql.io.impl;

import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.io.XOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class XOutputStreamImpl extends BufferedOutputStream implements XOutputStream {

    public XOutputStreamImpl(OutputStream out) {
        super(out);
    }

    public final void writeBytes(byte value[]) throws IOException {
        super.write(value, 0, value.length);
    }

    public final void writeBytes(int value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            super.write(value);
        }
    }

    public final void writeBytes(byte value[], int offset, int length) throws IOException {
        super.write(value, offset, length);
    }

    public final void writeInt(int value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            super.write(0x000000FF & (value >>> (i << 3)));
        }
    }

    public final void writeLong(long value, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            super.write((int) (0x00000000000000FF & (value >>> (i << 3))));
        }
    }

    public final void writeUnsignedLong(UnsignedLong value) throws IOException {
        final long length = value.longValue();
        if (length < 0) {
            writeLong(254, 1);
            writeLong(length, 8);
        } else if (length < 251L) {
            writeLong(length, 1);
        } else if (length < 65536L) {
            writeLong(252, 1);
            writeLong(length, 2);
        } else if (length < 16777216L) {
            writeLong(253, 1);
            writeLong(length, 3);
        } else {
            writeLong(254, 1);
            writeLong(length, 8);
        }
    }

    public final void writeLengthCodedString(StringColumn value) throws IOException {
        writeUnsignedLong(UnsignedLong.valueOf(value.getValue().length));
        writeFixedLengthString(value);
    }

    public final void writeFixedLengthString(StringColumn value) throws IOException {
        super.write(value.getValue());
    }

    public final void writeNullTerminatedString(StringColumn value) throws IOException {
        super.write(value.getValue());
        super.write(0);
    }
}