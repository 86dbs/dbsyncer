/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public class OracleException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OracleException(String message) {
        super(message);
    }

    public OracleException(String message, Throwable cause) {
        super(message, cause);
    }

    public OracleException(Throwable cause) {
        super(cause);
    }

    protected OracleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
