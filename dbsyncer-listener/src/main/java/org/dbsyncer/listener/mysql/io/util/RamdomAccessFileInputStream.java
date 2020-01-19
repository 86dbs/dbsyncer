package org.dbsyncer.listener.mysql.io.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class RamdomAccessFileInputStream extends InputStream {
	private final RandomAccessFile file;

	public RamdomAccessFileInputStream(File file) throws IOException {
		this.file = new RandomAccessFile(file, "r");
	}

	@Override
	public int available() throws IOException {
		final long fp = this.file.getFilePointer();
		return (int)(this.file.length() - fp);
	}

	@Override
	public void close() throws IOException {
		this.file.close();
	}

	@Override
	public long skip(long n) throws IOException {
		final long fp = this.file.getFilePointer();
		this.file.seek(fp + n);
		return n;
	}

	@Override
	public int read() throws IOException {
		return this.file.read();
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		return this.file.read(b, off, len);
	}
}
