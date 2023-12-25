package org.dbsyncer.connector.base;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:39
 */
public class ConnectorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ConnectorException(String message) {
        super(message);
    }

    public ConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectorException(Throwable cause) {
        super(cause);
    }

    protected ConnectorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
