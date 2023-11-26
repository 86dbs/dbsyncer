/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-11-22 23:55
 */
public class MySQLException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MySQLException(String message) {
        super(message);
    }

    public MySQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public MySQLException(Throwable cause) {
        super(cause);
    }

    protected MySQLException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
