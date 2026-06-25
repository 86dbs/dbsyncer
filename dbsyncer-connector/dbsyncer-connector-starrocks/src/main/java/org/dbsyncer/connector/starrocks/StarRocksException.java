/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.starrocks;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 03:00
 */
public class StarRocksException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StarRocksException(String message) {
        super(message);
    }

    public StarRocksException(String message, Throwable cause) {
        super(message, cause);
    }

    public StarRocksException(Throwable cause) {
        super(cause);
    }
}
