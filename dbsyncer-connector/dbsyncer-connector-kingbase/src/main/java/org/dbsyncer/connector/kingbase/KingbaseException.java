/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:30
 */
public class KingbaseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public KingbaseException(String message) {
        super(message);
    }

    public KingbaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public KingbaseException(Throwable cause) {
        super(cause);
    }
}
