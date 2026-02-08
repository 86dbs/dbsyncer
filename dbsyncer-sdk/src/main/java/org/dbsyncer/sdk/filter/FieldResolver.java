/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-11 17:36
 */
@FunctionalInterface
public interface FieldResolver<F> {

    Object getValue(F field);
}
