/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.sdk.parser.ddl.converter;

import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;

/**
 * 中间表示到目标数据库转换器接口
 */
public interface IRToTargetConverter {
    
    /**
     * 将中间表示转换为目标数据库DDL
     *
     * @param ir 中间表示
     * @return 目标数据库DDL语句
     */
    String convert(DDLIntermediateRepresentation ir);
}