/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.doris;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 23:50
 */
public class DorisException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DorisException(String message) {
        super(message);
    }

    public DorisException(String message, Throwable cause) {
        super(message, cause);
    }

    public DorisException(Throwable cause) {
        super(cause);
    }
}
