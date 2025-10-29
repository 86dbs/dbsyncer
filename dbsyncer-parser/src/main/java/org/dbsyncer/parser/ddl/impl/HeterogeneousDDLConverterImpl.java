/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.HeterogeneousDDLConverter;
import org.dbsyncer.parser.ddl.converter.MySQLToIRConverter;
import org.dbsyncer.parser.ddl.converter.SQLServerToIRConverter;
import org.dbsyncer.parser.ddl.converter.IRToMySQLConverter;
import org.dbsyncer.parser.ddl.converter.IRToSQLServerConverter;
import org.dbsyncer.parser.ddl.ir.DDLIntermediateRepresentation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 异构数据库DDL转换器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2025-11-26 20:10
 */
@Component
public class HeterogeneousDDLConverterImpl implements HeterogeneousDDLConverter {

    @Autowired
    private MySQLToIRConverter mySQLToIRConverter;
    
    @Autowired
    private SQLServerToIRConverter sqlServerToIRConverter;
    
    @Autowired
    private IRToMySQLConverter irToMySQLConverter;
    
    @Autowired
    private IRToSQLServerConverter irToSQLServerConverter;
    
    private final Map<String, String> converterMap = new HashMap<>();

    @PostConstruct
    public void init() {
        // 注册支持的转换器
        converterMap.put("MySQL->SqlServer", "MySQL->SqlServer");
        converterMap.put("SqlServer->MySQL", "SqlServer->MySQL");
    }

    @Override
    public String convert(String sourceConnectorType, String targetConnectorType, Alter alter) {
        // 1. 源DDL转中间表示
        DDLIntermediateRepresentation ir = parseToIR(sourceConnectorType, alter);
        // 2. 中间表示转目标DDL
        return generateFromIR(targetConnectorType, ir);
    }

    @Override
    public boolean supports(String sourceConnectorType, String targetConnectorType) {
        String key = sourceConnectorType + "->" + targetConnectorType;
        return converterMap.containsKey(key);
    }
    
    /**
     * 将源数据库DDL转换为中间表示
     *
     * @param sourceConnectorType 源数据库连接器类型
     * @param alter               源DDL语句解析对象
     * @return 中间表示
     */
    private DDLIntermediateRepresentation parseToIR(String sourceConnectorType, Alter alter) {
        if ("MySQL".equals(sourceConnectorType)) {
            return mySQLToIRConverter.convert(alter);
        } else if ("SqlServer".equals(sourceConnectorType)) {
            return sqlServerToIRConverter.convert(alter);
        }
        // 默认返回空的中间表示
        return new DDLIntermediateRepresentation();
    }

    /**
     * 将中间表示转换为目标数据库DDL
     *
     * @param targetConnectorType 目标数据库连接器类型
     * @param ir                  中间表示
     * @return 目标数据库DDL语句
     */
    private String generateFromIR(String targetConnectorType, DDLIntermediateRepresentation ir) {
        if ("MySQL".equals(targetConnectorType)) {
            return irToMySQLConverter.convert(ir);
        } else if ("SqlServer".equals(targetConnectorType)) {
            return irToSQLServerConverter.convert(ir);
        }
        // 默认返回空字符串
        return "";
    }
}