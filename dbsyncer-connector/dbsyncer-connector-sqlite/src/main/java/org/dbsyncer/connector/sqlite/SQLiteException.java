/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlite;

/**
 * @Author bble
 * @Version 1.0.0
 * @Date 2023-11-28 16:22
 */
public class SQLiteException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SQLiteException(String message) {
        super(message);
    }

    public SQLiteException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLiteException(Throwable cause) {
        super(cause);
    }

    protected SQLiteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
