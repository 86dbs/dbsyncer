package org.dbsyncer.sdk.parser.ddl.converter;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;

/**
 * 源数据库到中间表示转换器接口
 * 
 * 建议使用AbstractSourceToIRConverter抽象类作为基类，以简化实现
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