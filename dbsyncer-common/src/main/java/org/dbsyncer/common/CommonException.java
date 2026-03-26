package org.dbsyncer.common;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:39
 */
public class CommonException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CommonException(String message) {
        super(message);
    }

    public CommonException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommonException(Throwable cause) {
        super(cause);
    }

    protected CommonException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
