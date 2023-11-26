/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class FileException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public FileException(String message) {
        super(message);
    }

    public FileException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileException(Throwable cause) {
        super(cause);
    }

    protected FileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
