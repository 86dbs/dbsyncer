/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.dameng;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 02:00
 */
public class DamengException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DamengException(String message) {
        super(message);
    }

    public DamengException(String message, Throwable cause) {
        super(message, cause);
    }

    public DamengException(Throwable cause) {
        super(cause);
    }
}
