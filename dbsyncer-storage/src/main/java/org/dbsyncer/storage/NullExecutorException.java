package org.dbsyncer.storage;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2023/10/13 21:38
 */
public class NullExecutorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NullExecutorException(String message) {
        super(message);
    }
}