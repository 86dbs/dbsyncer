package org.dbsyncer.listener;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:39
 */
public class ListenerException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ListenerException(String message) {
        super(message);
    }

    public ListenerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ListenerException(Throwable cause) {
        super(cause);
    }

    protected ListenerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
