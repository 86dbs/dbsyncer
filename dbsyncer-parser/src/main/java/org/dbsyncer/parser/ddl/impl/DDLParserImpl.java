/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.ddl.alter.AddStrategy;
import org.dbsyncer.parser.ddl.alter.ChangeStrategy;
import org.dbsyncer.parser.ddl.alter.DropStrategy;
import org.dbsyncer.parser.ddl.alter.ModifyStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ddl解析器, 支持类型参考：{@link DDLOperationEnum}
 *
 * @author life
 * @version 1.0.0
 * @date 2023/9/19 22:38
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorFactory connectorFactory;

    private final Map<AlterOperation, AlterStrategy> STRATEGIES = new ConcurrentHashMap();

    @PostConstruct
    private void init() {
        STRATEGIES.putIfAbsent(AlterOperation.MODIFY, new ModifyStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.ADD, new AddStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.CHANGE, new ChangeStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.DROP, new DropStrategy());
    }

    @Override
    public DDLConfig parseDDlConfig(ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException {
        DDLConfig ddlConfig = new DDLConfig();
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Alter && connectorService instanceof Database) {
            Alter alter = (Alter) statement;
            Database database = (Database) connectorService;
            String quotation = database.buildSqlWithQuotation();
            // 替换成目标表名
            alter.getTable().setName(quotation + tableGroup.getTargetTable().getName() + quotation);
            ddlConfig.setSql(alter.toString());
            for (AlterExpression expression : alter.getAlterExpressions()) {
                STRATEGIES.computeIfPresent(expression.getOperation(), (k, strategy) -> {
                    strategy.parse(expression, ddlConfig, tableGroup.getFieldMapping());
                    return strategy;
                });
            }
        }
        return ddlConfig;
    }

    @Override
    public void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig) {
        switch (targetDDLConfig.getDdlOperationEnum()) {
            case ALTER_MODIFY:
                updateFieldMapping(tableGroup, targetDDLConfig.getSourceColumnName());
                break;
            case ALTER_ADD:
                appendFieldMappings(tableGroup, targetDDLConfig.getAddFieldNames());
                break;
            case ALTER_CHANGE:
                renameFieldMapping(tableGroup, targetDDLConfig.getSourceColumnName(), targetDDLConfig.getChangedColumnName());
                break;
            case ALTER_DROP:
                removeFieldMappings(tableGroup, targetDDLConfig.getRemoveFieldNames());
                break;
            default:
                break;
        }
    }

    private void updateFieldMapping(TableGroup tableGroup, String updateFieldName) {
        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null && StringUtil.equals(updateFieldName, source.getName())) {
                sourceFiledMap.computeIfPresent(updateFieldName, (k, field) -> {
                    fieldMapping.setSource(field);
                    return field;
                });
                if (target != null && StringUtil.equals(updateFieldName, target.getName())) {
                    targetFiledMap.computeIfPresent(updateFieldName, (k, field) -> {
                        fieldMapping.setTarget(field);
                        return field;
                    });
                }
            }
        }
    }

    private void renameFieldMapping(TableGroup tableGroup, String oldFieldName, String newFieldName) {
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null && StringUtil.equals(oldFieldName, source.getName())) {
                source.setName(newFieldName);
                if (target != null && StringUtil.equals(oldFieldName, target.getName())) {
                    target.setName(newFieldName);
                }
            }
        }
    }

    private void removeFieldMappings(TableGroup tableGroup, List<String> removeFieldNames) {
        Iterator<FieldMapping> iterator = tableGroup.getFieldMapping().iterator();
        while (iterator.hasNext()) {
            FieldMapping fieldMapping = iterator.next();
            Field source = fieldMapping.getSource();
            if (source != null && removeFieldNames.contains(source.getName())) {
                iterator.remove();
            }
        }
    }

    private void appendFieldMappings(TableGroup tableGroup, List<String> addFieldNames) {
        List<FieldMapping> fieldMappings = tableGroup.getFieldMapping();
        Iterator<String> iterator = addFieldNames.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            for (FieldMapping fieldMapping : fieldMappings) {
                Field source = fieldMapping.getSource();
                Field target = fieldMapping.getTarget();
                // 检查重复字段
                if (source != null && target != null && StringUtil.equals(source.getName(), name) && StringUtil.equals(target.getName(), name)) {
                    iterator.remove();
                }
            }
        }
        if (CollectionUtils.isEmpty(addFieldNames)) {
            return;
        }

        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        if (CollectionUtils.isEmpty(sourceFiledMap) || CollectionUtils.isEmpty(targetFiledMap)) {
            return;
        }
        addFieldNames.forEach(newFieldName -> {
            if (sourceFiledMap.containsKey(newFieldName) && targetFiledMap.containsKey(newFieldName)) {
                fieldMappings.add(new FieldMapping(sourceFiledMap.get(newFieldName), targetFiledMap.get(newFieldName)));
            }
        });
    }

}