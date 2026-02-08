/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public class HttpException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public HttpException(String message) {
        super(message);
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpException(Throwable cause) {
        super(cause);
    }

}
