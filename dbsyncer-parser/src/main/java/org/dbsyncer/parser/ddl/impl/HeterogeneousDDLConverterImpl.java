/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.parser.ddl.HeterogeneousDDLConverter;
import org.dbsyncer.sdk.config.DDLConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    private List<HeterogeneousDDLConverter> converters;
    
    private final ConcurrentMap<String, HeterogeneousDDLConverter> converterMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        for (HeterogeneousDDLConverter converter : converters) {
            // 跳过自身
            if (!(converter instanceof HeterogeneousDDLConverterImpl)) {
                // 注册具体的转换器
                // 通过supports方法测试支持的转换类型
                // 这里简化处理，实际应该有更完善的注册机制
                // 为演示目的，我们假设每个转换器类名包含源和目标类型信息
                String className = converter.getClass().getSimpleName();
                if (className.contains("MySQLToSQLServer")) {
                    converterMap.put("MySQL->SqlServer", converter);
                } else if (className.contains("SQLServerToMySQL")) {
                    converterMap.put("SqlServer->MySQL", converter);
                }
                // 可以继续添加其他转换器
            }
        }
    }

    @Override
    public String convert(String sourceConnectorType, String targetConnectorType, Alter alter, DDLConfig ddlConfig) {
        String key = sourceConnectorType + "->" + targetConnectorType;
        HeterogeneousDDLConverter converter = converterMap.get(key);
        
        if (converter != null && converter.supports(sourceConnectorType, targetConnectorType)) {
            return converter.convert(sourceConnectorType, targetConnectorType, alter, ddlConfig);
        }
        
        // 如果没有找到特定的转换器，返回默认转换
        return alter.toString();
    }

    @Override
    public boolean supports(String sourceConnectorType, String targetConnectorType) {
        String key = sourceConnectorType + "->" + targetConnectorType;
        HeterogeneousDDLConverter converter = converterMap.get(key);
        return converter != null && converter.supports(sourceConnectorType, targetConnectorType);
    }
}