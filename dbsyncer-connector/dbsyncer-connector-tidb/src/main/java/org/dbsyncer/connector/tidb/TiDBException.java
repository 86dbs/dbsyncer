/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.tidb;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 00:10
 */
public class TiDBException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TiDBException(String message) {
        super(message);
    }

    public TiDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public TiDBException(Throwable cause) {
        super(cause);
    }
}
