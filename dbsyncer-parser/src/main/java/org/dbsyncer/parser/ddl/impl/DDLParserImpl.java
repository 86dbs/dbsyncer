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
 * alter情况
 * <ol>
 *     <li>只是修改字段的属性值</li>
 *     <li>修改字段的名称</li>
 * </ol>
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
    public DDLConfig parseDDlConfig(ConnectorService connectorService, TableGroup tableGroup, String sql) {
        // 替换为目标库执行SQL
        DDLConfig ddlConfig = new DDLConfig();
        try {
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
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
        return ddlConfig;
    }

    @Override
    public void refreshFiledMappings(TableGroup tableGroup, DDLConfig targetDDLConfig) {
        switch (targetDDLConfig.getDdlOperationEnum()) {
            case ALTER_ADD:
                appendFieldMappings(tableGroup, targetDDLConfig.getAddFieldNames());
                break;
            case ALTER_DROP:
                removeFieldMappings(tableGroup, targetDDLConfig.getRemoveFieldNames());
                break;
            case ALTER_CHANGE:
                renameFieldMapping(tableGroup, targetDDLConfig.getSourceColumnName(), targetDDLConfig.getChangedColumnName());
                break;
            case ALTER_MODIFY:
                // 可以忽略，仅修改字段属性，名称未变
                break;
            default:
                break;
        }
    }

    private void renameFieldMapping(TableGroup tableGroup, String oldFieldName, String newFieldName) {
        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream().collect(Collectors.toMap(Field::getName, filed -> filed));
        for (FieldMapping fieldMapping : tableGroup.getFieldMapping()) {
            Field source = fieldMapping.getSource();
            if (source != null && StringUtil.equals(oldFieldName, source.getName())) {
                source.setName(newFieldName);
                sourceFiledMap.computeIfPresent(newFieldName, (k, field) -> {
                    fieldMapping.setSource(field);
                    return field;
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