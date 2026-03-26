/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.kafka;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-01-11 20:17
 */
public class KafkaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(Throwable cause) {
        super(cause);
    }
}
