package org.dbsyncer.cache;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
public class CacheException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CacheException(String message) {
        super(message);
    }

    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheException(Throwable cause) {
        super(cause);
    }

    protected CacheException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}