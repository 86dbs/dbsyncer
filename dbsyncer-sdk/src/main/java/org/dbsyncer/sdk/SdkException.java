/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-16 00:51
 */
public class SdkException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SdkException(String message) {
        super(message);
    }

    public SdkException(String message, Throwable cause) {
        super(message, cause);
    }

    public SdkException(Throwable cause) {
        super(cause);
    }

    protected SdkException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}