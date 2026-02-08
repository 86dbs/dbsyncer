package org.dbsyncer.manager;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:39
 */
public class ManagerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ManagerException(String message) {
        super(message);
    }

    public ManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ManagerException(Throwable cause) {
        super(cause);
    }

    protected ManagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
