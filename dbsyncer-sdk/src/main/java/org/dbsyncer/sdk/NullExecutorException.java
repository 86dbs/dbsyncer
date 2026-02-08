/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-10-13 21:38
 */
public class NullExecutorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NullExecutorException(String message) {
        super(message);
    }
}
