/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.h2;

import org.dbsyncer.sdk.SdkException;

/**
 * H2连接器异常
 */
public final class H2Exception extends SdkException {

    public H2Exception(String message) {
        super(message);
    }

    public H2Exception(Throwable cause) {
        super(cause);
    }
}
