package org.dbsyncer.listener.mysql.io;

import java.io.IOException;

public class ExceedLimitException extends IOException {
	private static final long serialVersionUID = -5580022370029510002L;

	public ExceedLimitException() {
	}

	public ExceedLimitException(String message) {
		super(message);
	}
}
