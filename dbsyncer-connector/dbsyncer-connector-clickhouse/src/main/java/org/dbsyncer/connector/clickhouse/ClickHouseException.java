/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.clickhouse;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-05-29 23:50
 */
public final class ClickHouseException extends RuntimeException {

    public ClickHouseException(String message) {
        super(message);
    }

    public ClickHouseException(Throwable cause) {
        super(cause);
    }
}
