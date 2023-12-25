/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter;

/**
 * 值比较器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2019-11-17 23:56
 */
public interface CompareFilter {

    boolean compare(String value, String filterValue);

}