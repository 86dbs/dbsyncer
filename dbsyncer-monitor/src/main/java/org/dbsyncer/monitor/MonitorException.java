package org.dbsyncer.monitor;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/7/23 22:39
 */
public class MonitorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MonitorException(String message) {
        super(message);
    }

    public MonitorException(String message, Throwable cause) {
        super(message, cause);
    }

    public MonitorException(Throwable cause) {
        super(cause);
    }

    protected MonitorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
