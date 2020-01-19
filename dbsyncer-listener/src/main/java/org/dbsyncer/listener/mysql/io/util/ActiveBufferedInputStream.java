package org.dbsyncer.listener.mysql.io.util;

import org.dbsyncer.listener.mysql.common.util.XThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class ActiveBufferedInputStream extends InputStream implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveBufferedInputStream.class);

    private static final int DEFAULT_CAPACITY = 2 * 1024 * 1024;

    private final Thread worker;
    private final InputStream is;
    private volatile IOException exception;
    private final ByteRingBuffer ringBuffer;
    private final ThreadFactory threadFactory;
    private final ReentrantLock lock = new ReentrantLock(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Condition bufferNotFull = this.lock.newCondition();
    private final Condition bufferNotEmpty = this.lock.newCondition();

    public ActiveBufferedInputStream(InputStream is) {
        this(is, DEFAULT_CAPACITY);
    }

    public ActiveBufferedInputStream(InputStream is, int size) {
        this(is, size, new XThreadFactory("bis-active", true));
    }

    public ActiveBufferedInputStream(InputStream is, int size, ThreadFactory tf) {
        this.is = is;
        this.threadFactory = tf;
        this.ringBuffer = new ByteRingBuffer(size);

        this.worker = this.threadFactory.newThread(this);
        this.worker.start();
    }

    public void run() {
        try {
            final byte[] buffer = new byte[512 * 1024];
            while (!this.closed.get()) {
                int r = this.is.read(buffer, 0, buffer.length);
                if (r < 0) throw new EOFException();

                int offset = 0;
                while (r > 0) {
                    final int w = write(buffer, offset, r);
                    r -= w;
                    offset += w;
                }
            }
        } catch (IOException e) {
            this.exception = e;

            this.lock.lock();
            try {
                this.bufferNotFull.signalAll();
                this.bufferNotEmpty.signalAll();
            } finally {
                this.lock.unlock();
            }
        } catch (Exception e) {
            LOGGER.error("failed to transfer data", e);
        } finally {
            if (!this.closed.get()) {
                try {
                    close();
                } catch (IOException e) {
                    LOGGER.error("failed to close is", e);
                }
            }
        }
    }

    @Override
    public int available() throws IOException {
        return this.ringBuffer.size();
    }

    @Override
    public void close() throws IOException {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }

        try {
            this.is.close();
        } finally {
            this.lock.lock();
            try {
                this.bufferNotFull.signalAll();
                this.bufferNotEmpty.signalAll();
            } finally {
                this.lock.unlock();
            }
        }
    }

    @Override
    public int read() throws IOException {
        this.lock.lock();
        try {
            while (this.ringBuffer.isEmpty()) {
                if (this.exception != null) throw this.exception;
                this.bufferNotEmpty.awaitUninterruptibly();
                if (this.closed.get() && this.ringBuffer.isEmpty()) throw new EOFException();
            }

            final int r = this.ringBuffer.read();
            this.bufferNotFull.signal();
            return r;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        this.lock.lock();
        try {
            while (this.ringBuffer.isEmpty()) {
                if (this.exception != null) throw this.exception;
                this.bufferNotEmpty.awaitUninterruptibly();
                if (this.closed.get() && this.ringBuffer.isEmpty()) throw new EOFException();
            }

            final int r = this.ringBuffer.read(b, off, len);
            this.bufferNotFull.signal();
            return r;
        } finally {
            this.lock.unlock();
        }
    }

    public int write(byte b[], int off, int len) throws IOException {
        this.lock.lock();
        try {
            while (this.ringBuffer.isFull()) {
                this.bufferNotFull.awaitUninterruptibly();
                if (this.closed.get()) throw new EOFException();
            }

            final int w = this.ringBuffer.write(b, off, len);
            this.bufferNotEmpty.signal();
            return w;
        } finally {
            this.lock.unlock();
        }
    }

    private final class ByteRingBuffer {
        private int size;
        private int head; // Write
        private int tail; // Read
        private final byte[] buffer;

        public ByteRingBuffer(int capacity) {
            this.buffer = new byte[capacity];
        }

        public int size() {
            return this.size;
        }

        public boolean isEmpty() {
            return this.size == 0;
        }

        public boolean isFull() {
            return this.size == this.buffer.length;
        }

        public int read() {
            final int r = this.buffer[this.tail] & 0xFF;

            this.tail = (this.tail + 1) % this.buffer.length;
            this.size -= 1;
            return r;
        }

        public int read(byte b[], int off, int len) {
            final int r = Math.min(this.size, len);
            if (this.head > this.tail) {
                System.arraycopy(this.buffer, this.tail, b, off, r);
            } else {
                final int r1 = Math.min(this.buffer.length - this.tail, r);
                System.arraycopy(this.buffer, this.tail, b, off, r1);
                if (r1 < r) System.arraycopy(this.buffer, 0, b, off + r1, r - r1);
            }

            this.tail = (this.tail + r) % this.buffer.length;
            this.size -= r;
            return r;
        }

        public int write(byte b[], int off, int len) {
            final int w = Math.min(this.buffer.length - this.size, len);
            if (this.head < this.tail) {
                System.arraycopy(b, off, this.buffer, this.head, w);
            } else {
                final int w1 = Math.min(this.buffer.length - this.head, w);
                System.arraycopy(b, off, this.buffer, this.head, w1);
                if (w1 < w) System.arraycopy(b, off + w1, this.buffer, 0, w - w1);
            }

            this.head = (this.head + w) % this.buffer.length;
            this.size += w;
            return w;
        }
    }
}
