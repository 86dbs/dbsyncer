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
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.ddl.HeterogeneousDDLConverter;
import org.dbsyncer.parser.ddl.alter.AddStrategy;
import org.dbsyncer.parser.ddl.alter.ChangeStrategy;
import org.dbsyncer.parser.ddl.alter.DropStrategy;
import org.dbsyncer.parser.ddl.alter.ModifyStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;
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
import java.util.Set;
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
    private final Map<AlterOperation, AlterStrategy> STRATEGIES = new ConcurrentHashMap();

    @Resource
    private HeterogeneousDDLConverter heterogeneousDDLConverter;

    @PostConstruct
    private void init() {
        STRATEGIES.putIfAbsent(AlterOperation.MODIFY, new ModifyStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.ADD, new AddStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.CHANGE, new ChangeStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.DROP, new DropStrategy());
    }

    @Override
    public DDLConfig parse(ConnectorService connectorService, TableGroup tableGroup, String sql) throws JSQLParserException {
        DDLConfig ddlConfig = new DDLConfig();
        logger.info("ddl:{}", sql);
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Alter && connectorService instanceof Database) {
            Alter alter = (Alter) statement;
            // 使用SQL模板构建带引号的表名
            String quotedTableName = tableGroup.getTargetTable().getName();
            if (connectorService instanceof AbstractDatabaseConnector) {
                AbstractDatabaseConnector dbConnector = (AbstractDatabaseConnector) connectorService;
                quotedTableName = dbConnector.sqlTemplate.buildQuotedTableName(tableGroup.getTargetTable().getName());
            }
            // 替换成目标表名
            alter.getTable().setName(quotedTableName);

            // 设置源和目标连接器类型
            // 注意：这里需要从上下文获取真实的连接器类型
            // 为演示目的，我们使用占位符值
            ddlConfig.setSourceConnectorType("MySQL"); // 需要从TableGroup或Mapping中获取
            ddlConfig.setTargetConnectorType(connectorService.getConnectorType());

            // 对于异构数据库，进行DDL语法转换
            String targetSql = alter.toString();
            // 如果是异构数据库，尝试进行转换
            if (heterogeneousDDLConverter != null && ddlConfig.getSourceConnectorType() != null && ddlConfig.getTargetConnectorType() != null) {
                // 检查是否支持转换
                if (heterogeneousDDLConverter.supports(ddlConfig.getSourceConnectorType(), ddlConfig.getTargetConnectorType())) {
                    targetSql = heterogeneousDDLConverter.convert(ddlConfig.getSourceConnectorType(), ddlConfig.getTargetConnectorType(), alter, ddlConfig);
                }
            }

            ddlConfig.setSql(targetSql);

            for (AlterExpression expression : alter.getAlterExpressions()) {
                STRATEGIES.computeIfPresent(expression.getOperation(), (k, strategy) -> {
                    strategy.parse(expression, ddlConfig);
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
                updateFieldMapping(tableGroup, targetDDLConfig.getModifiedFieldNames());
                break;
            case ALTER_ADD:
                appendFieldMappings(tableGroup, targetDDLConfig.getAddedFieldNames());
                break;
            case ALTER_CHANGE:
                renameFieldMapping(tableGroup, targetDDLConfig.getChangedFieldNames());
                break;
            case ALTER_DROP:
                removeFieldMappings(tableGroup, targetDDLConfig.getDroppedFieldNames());
                break;
            default:
                break;
        }
    }

    private void updateFieldMapping(TableGroup tableGroup, List<String> modifiedFieldNames) {
        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null) {
                String modifiedName = source.getName();
                if (!modifiedFieldNames.contains(modifiedName)) {
                    continue;
                }
                sourceFiledMap.computeIfPresent(modifiedName, (k, field) -> {
                    fieldMapping.setSource(field);
                    return field;
                });
                if (target != null && StringUtil.equals(modifiedName, target.getName())) {
                    targetFiledMap.computeIfPresent(modifiedName, (k, field) -> {
                        fieldMapping.setTarget(field);
                        return field;
                    });
                }
            }
        }
    }

    private void renameFieldMapping(TableGroup tableGroup, Map<String, String> changedFieldNames) {
        Set<String> oldNames = changedFieldNames.keySet();
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            // 支持1对多场景
            if (source != null) {
                String oldFieldName = source.getName();
                if (!oldNames.contains(oldFieldName)) {
                    continue;
                }
                changedFieldNames.computeIfPresent(oldFieldName, (k, newName) -> {
                    source.setName(newName);
                    if (target != null && StringUtil.equals(oldFieldName, target.getName())) {
                        target.setName(newName);
                    }
                    return newName;
                });
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

    private void appendFieldMappings(TableGroup tableGroup, List<String> addedFieldNames) {
        List<FieldMapping> fieldMappings = tableGroup.getFieldMapping();
        Iterator<String> iterator = addedFieldNames.iterator();
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
        if (CollectionUtils.isEmpty(addedFieldNames)) {
            return;
        }

        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        if (CollectionUtils.isEmpty(sourceFiledMap) || CollectionUtils.isEmpty(targetFiledMap)) {
            return;
        }
        addedFieldNames.forEach(newFieldName -> {
            if (sourceFiledMap.containsKey(newFieldName) && targetFiledMap.containsKey(newFieldName)) {
                fieldMappings.add(new FieldMapping(sourceFiledMap.get(newFieldName), targetFiledMap.get(newFieldName)));
            }
        });
    }

}