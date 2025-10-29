/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;

/**
 * 源数据库到中间表示转换器接口
 */
public interface SourceToIRConverter {
    
    /**
     * 将源数据库DDL转换为中间表示
     *
     * @param alter 源DDL语句解析对象
     * @return 中间表示
     */
    DDLIntermediateRepresentation convert(Alter alter);
}