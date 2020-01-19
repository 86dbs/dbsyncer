package org.dbsyncer.storage;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/11/10 21:38
 */
public class StorageException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }

    protected StorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
