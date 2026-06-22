/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbase;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-04 00:20
 */
public class OceanBaseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OceanBaseException(String message) {
        super(message);
    }

    public OceanBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public OceanBaseException(Throwable cause) {
        super(cause);
    }

    protected OceanBaseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
