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
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.parser.ddl.converter.IRToTargetConverter;
import org.dbsyncer.sdk.parser.ddl.converter.SourceToIRConverter;
import org.dbsyncer.sdk.parser.ddl.ir.DDLIntermediateRepresentation;
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
 */
@Component
public class DDLParserImpl implements DDLParser {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<AlterOperation, AlterStrategy> STRATEGIES = new ConcurrentHashMap<>();

    @Resource
    private ConnectorFactory connectorFactory;

    @PostConstruct
    private void init() {
        STRATEGIES.putIfAbsent(AlterOperation.MODIFY, new ModifyStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.ADD, new AddStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.CHANGE, new ChangeStrategy());
        STRATEGIES.putIfAbsent(AlterOperation.DROP, new DropStrategy());
    }

    @Override
    public DDLConfig parse(ConnectorService connectorService, TableGroup tableGroup, String sql)
            throws JSQLParserException {
        DDLConfig ddlConfig = new DDLConfig();
        logger.info("source ddl: {}", sql);
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Alter && connectorService instanceof Database) {
            Alter alter = (Alter) statement;

            // 设置源和目标连接器类型
            // 从TableGroup中获取ProfileComponent，再获取源和目标连接器配置
            Mapping mapping = tableGroup.profileComponent.getMapping(tableGroup.getMappingId());
            ConnectorConfig sourceConnectorConfig = tableGroup.profileComponent
                    .getConnector(mapping.getSourceConnectorId()).getConfig();
            ConnectorConfig targetConnectorConfig = tableGroup.profileComponent
                    .getConnector(mapping.getTargetConnectorId()).getConfig();
            String sourceConnectorType = sourceConnectorConfig.getConnectorType();
            String targetConnectorType = targetConnectorConfig.getConnectorType();

            // 获取目标表名（原始表名，不带引号）
            String targetTableName = tableGroup.getTargetTable().getName();
            
            // 获取目标数据库的 schema（如果配置了的话）
            String targetSchema = null;
            if (targetConnectorConfig instanceof org.dbsyncer.sdk.config.DatabaseConfig) {
                org.dbsyncer.sdk.config.DatabaseConfig targetDatabaseConfig = 
                    (org.dbsyncer.sdk.config.DatabaseConfig) targetConnectorConfig;
                targetSchema = targetDatabaseConfig.getSchema();
            }

            // 从源连接器获取源到IR转换器（用于统一处理操作类型映射，如同构数据库的 ALTER -> MODIFY）
            ConnectorService sourceConnectorService = connectorFactory.getConnectorService(sourceConnectorConfig);
            SourceToIRConverter sourceToIRConverter = sourceConnectorService.getSourceToIRConverter();

            String targetSql;
            boolean isHeterogeneous = !StringUtil.equals(sourceConnectorType, targetConnectorType);
            
            if (isHeterogeneous) {
                // 对于异构数据库，进行DDL语法转换
                // 使用原始表名（不带引号），因为IRToTargetConverter的buildAddColumnSql会自己加引号
                alter.getTable().setName(targetTableName);

                // 从目标连接器获取IR到目标转换器
                IRToTargetConverter irToTargetConverter = connectorService.getIRToTargetConverter();

                // 检查转换器是否有效
                if (sourceToIRConverter == null || irToTargetConverter == null) {
                    throw new IllegalArgumentException(
                            String.format("无法获取从 %s 到 %s 的转换器", sourceConnectorType, targetConnectorType));
                }

                // 直接转换源DDL到目标DDL
                // 1. 源DDL转中间表示（注意：convert方法会修改AlterExpression的操作类型）
                DDLIntermediateRepresentation ir = sourceToIRConverter.convert(alter);
                // 2. 确保IR中的表名是原始表名（不带引号）
                ir.setTableName(targetTableName);
                // 3. 设置目标数据库的 schema
                ir.setSchema(targetSchema);
                // 4. 中间表示转目标DDL
                targetSql = irToTargetConverter.convert(ir);
            } else {
                // 对于同构数据库，直接使用原生SQL，不需要转换
                // 但是需要先替换表名为目标表名，否则会执行到源库
                alter.getTable().setName(targetTableName);
                
                // 但是需要调用 SourceToIRConverter.convert 来统一处理操作类型映射
                // 例如：SQL Server 的 ALTER COLUMN 需要映射为 MODIFY，才能被策略模式处理
                sourceToIRConverter.convert(alter);
                
                // 在替换表名和转换操作类型后，生成目标SQL
                targetSql = alter.toString();
            }

            ddlConfig.setSql(targetSql);
            logger.info("target ddl: {}", targetSql);

            // 统一使用策略模式处理（同构和异构数据库都适用）
            // 策略模式的作用是解析DDL提取字段信息，用于更新字段映射（FieldMapping）
            // SourceToIRConverter.convert 已经修改了 AlterExpression 的操作类型（如 ALTER -> MODIFY）
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
                logger.error("不支持的DDL操作类型: {}, DDL SQL: {}, 无法更新字段映射", 
                        targetDDLConfig.getDdlOperationEnum(), targetDDLConfig.getSql());
                throw new RuntimeException("不支持的DDL操作类型");
        }
    }

    private void updateFieldMapping(TableGroup tableGroup, List<String> modifiedFieldNames) {
        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));
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

    private void appendFieldMappings(TableGroup tableGroup, List<String> addedFieldNames) {
        if (CollectionUtils.isEmpty(addedFieldNames)) {
            return;
        }

        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));

        for (String addedFieldName : addedFieldNames) {
            Field source = sourceFiledMap.get(addedFieldName);
            Field target = targetFiledMap.get(addedFieldName);
            if (source != null && target != null) {
                tableGroup.getFieldMapping().add(new FieldMapping(source, target));
            }
        }
    }

    private void renameFieldMapping(TableGroup tableGroup, Map<String, String> changedFieldNames) {
        if (changedFieldNames.isEmpty()) {
            return;
        }

        Map<String, Field> sourceFiledMap = tableGroup.getSourceTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));
        Map<String, Field> targetFiledMap = tableGroup.getTargetTable().getColumn().stream()
                .collect(Collectors.toMap(Field::getName, filed -> filed));

        Set<Map.Entry<String, String>> entries = changedFieldNames.entrySet();
        Iterator<FieldMapping> iterator = tableGroup.getFieldMapping().iterator();
        while (iterator.hasNext()) {
            FieldMapping fieldMapping = iterator.next();
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            for (Map.Entry<String, String> entry : entries) {
                String oldName = entry.getKey();
                String newName = entry.getValue();
                if (source != null && StringUtil.equals(oldName, source.getName())) {
                    Field newSourceFiled = sourceFiledMap.get(newName);
                    if (newSourceFiled != null) {
                        fieldMapping.setSource(newSourceFiled);
                    } else {
                        iterator.remove();
                    }
                }
                if (target != null && StringUtil.equals(oldName, target.getName())) {
                    Field newTargetFiled = targetFiledMap.get(newName);
                    if (newTargetFiled != null) {
                        fieldMapping.setTarget(newTargetFiled);
                    } else {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private void removeFieldMappings(TableGroup tableGroup, List<String> droppedFieldNames) {
        if (CollectionUtils.isEmpty(droppedFieldNames)) {
            return;
        }

        Iterator<FieldMapping> iterator = tableGroup.getFieldMapping().iterator();
        while (iterator.hasNext()) {
            FieldMapping fieldMapping = iterator.next();
            Field source = fieldMapping.getSource();
            Field target = fieldMapping.getTarget();
            for (String droppedFieldName : droppedFieldNames) {
                if ((source != null && StringUtil.equals(droppedFieldName, source.getName()))
                        || (target != null && StringUtil.equals(droppedFieldName, target.getName()))) {
                    iterator.remove();
                    break;
                }
            }
        }
    }
}