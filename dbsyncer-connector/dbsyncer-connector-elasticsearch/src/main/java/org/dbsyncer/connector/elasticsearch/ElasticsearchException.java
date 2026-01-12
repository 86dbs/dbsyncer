/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class ElasticsearchException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ElasticsearchException(String message) {
        super(message);
    }

    public ElasticsearchException(Throwable cause) {
        super(cause);
    }

}
