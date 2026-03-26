package org.dbsyncer.common;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/12/04 22:39
 */
public class QueueOverflowException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueueOverflowException(String message) {
        super(message);
    }

    public QueueOverflowException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueOverflowException(Throwable cause) {
        super(cause);
    }

    protected QueueOverflowException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
