/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.rocketmq;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-07 01:00
 */
public class RocketMQException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RocketMQException(String message) {
        super(message);
    }

    public RocketMQException(String message, Throwable cause) {
        super(message, cause);
    }

    public RocketMQException(Throwable cause) {
        super(cause);
    }
}
