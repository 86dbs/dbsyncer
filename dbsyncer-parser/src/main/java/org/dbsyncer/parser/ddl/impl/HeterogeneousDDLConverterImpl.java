/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.HeterogeneousDDLConverter;
import org.dbsyncer.parser.ddl.converter.SourceToIRConverter;
import org.dbsyncer.parser.ddl.converter.IRToTargetConverter;
import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.springframework.stereotype.Component;

/**
 * 异构数据库DDL转换器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 20:10
 */
@Component
public class HeterogeneousDDLConverterImpl implements HeterogeneousDDLConverter {

    @Override
    public String convert(SourceToIRConverter sourceToIRConverter, IRToTargetConverter irToTargetConverter, Alter alter) {
        // 1. 源DDL转中间表示
        DDLIntermediateRepresentation ir = sourceToIRConverter.convert(alter);
        // 2. 中间表示转目标DDL
        return irToTargetConverter.convert(ir);
    }

    @Override
    public boolean supports(String sourceConnectorType, String targetConnectorType) {
        // 在新的设计中，支持的转换由传入的具体转换器决定
        // 这里简单返回true，实际支持性应该在调用时通过传入的转换器来判断
        return true;
    }
}