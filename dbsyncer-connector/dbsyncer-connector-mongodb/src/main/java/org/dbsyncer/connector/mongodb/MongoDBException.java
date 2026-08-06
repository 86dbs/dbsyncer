/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public class MongoDBException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MongoDBException(String message) {
        super(message);
    }

    public MongoDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public MongoDBException(Throwable cause) {
        super(cause);
    }
}
