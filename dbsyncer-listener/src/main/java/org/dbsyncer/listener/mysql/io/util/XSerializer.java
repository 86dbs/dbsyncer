package org.dbsyncer.listener.mysql.io.util;

import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.glossary.column.StringColumn;
import org.dbsyncer.listener.mysql.io.XOutputStream;
import org.dbsyncer.listener.mysql.io.impl.XOutputStreamImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class XSerializer implements XOutputStream {
    private final XOutputStream tos;
    private final ByteArrayOutputStream bos;

    public XSerializer() {
        this.bos = new ByteArrayOutputStream();
        this.tos = new XOutputStreamImpl(this.bos);
    }

    public XSerializer(int size) {
        this.bos = new ByteArrayOutputStream(size);
        this.tos = new XOutputStreamImpl(this.bos);
    }

    public byte[] toByteArray() {
        flush();
        return this.bos.toByteArray();
    }

    public void flush() {
        try {
            this.tos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        try {
            this.tos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeBytes(byte[] value) {
        try {
            this.tos.writeBytes(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeBytes(int value, int length) {
        try {
            this.tos.writeBytes(value, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeBytes(byte[] value, int offset, int length) {
        try {
            this.tos.writeBytes(value, offset, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeInt(int value, int length) {
        try {
            this.tos.writeInt(value, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeLong(long value, int length) {
        try {
            this.tos.writeLong(value, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeUnsignedLong(UnsignedLong value) {
        try {
            this.tos.writeUnsignedLong(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeLengthCodedString(StringColumn value) {
        try {
            this.tos.writeLengthCodedString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeFixedLengthString(StringColumn value) {
        try {
            this.tos.writeFixedLengthString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeNullTerminatedString(StringColumn value) {
        try {
            this.tos.writeNullTerminatedString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
