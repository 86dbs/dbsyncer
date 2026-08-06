/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.duckdb;

/**
 * DuckDB 连接器异常
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-16 10:00
 */
public class DuckDBException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DuckDBException(String message) {
        super(message);
    }

    public DuckDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuckDBException(Throwable cause) {
        super(cause);
    }
}
