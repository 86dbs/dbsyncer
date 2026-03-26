package org.dbsyncer.parser;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/28 22:39
 */
public class ParserException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ParserException(String message) {
        super(message);
    }

    public ParserException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParserException(Throwable cause) {
        super(cause);
    }

    protected ParserException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
