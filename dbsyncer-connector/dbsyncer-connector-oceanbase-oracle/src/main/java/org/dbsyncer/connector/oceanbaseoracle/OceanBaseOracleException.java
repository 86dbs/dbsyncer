/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oceanbaseoracle;

/**
 * OceanBase Oracle 模式连接器异常
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 14:30
 */
public class OceanBaseOracleException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OceanBaseOracleException(String message) {
        super(message);
    }

    public OceanBaseOracleException(String message, Throwable cause) {
        super(message, cause);
    }

    public OceanBaseOracleException(Throwable cause) {
        super(cause);
    }
}
