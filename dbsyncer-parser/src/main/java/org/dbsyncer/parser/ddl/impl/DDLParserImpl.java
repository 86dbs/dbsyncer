package org.dbsyncer.parser.ddl.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.sdk.config.DDLConfig;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.parser.ddl.AlterStrategy;
import org.dbsyncer.parser.ddl.DDLParser;
import org.dbsyncer.parser.ddl.alter.AddStrategy;
import org.dbsyncer.parser.ddl.alter.ChangeStrategy;
import org.dbsyncer.parser.ddl.alter.DropStrategy;
import org.dbsyncer.parser.ddl.alter.ModifyStrategy;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.sdk.enums.DDLOperationEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.spi.ConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    public DDLConfig parseDDlConfig(String sql, String targetConnectorType, String targetTableName, List<FieldMapping> originalFieldMappings) {
        ConnectorService connectorService = connectorFactory.getConnectorService(targetConnectorType);
        // 替换为目标库执行SQL
        DDLConfig ddlConfig = new DDLConfig();
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                Database database = (Database) connectorService;
                String quotation = database.buildSqlWithQuotation();
                // 替换成目标表名
                alter.getTable().setName(new StringBuilder(quotation).append(targetTableName).append(quotation).toString());
                ddlConfig.setSql(alter.toString());
                for (AlterExpression expression : alter.getAlterExpressions()) {
                    if (STRATEGIES.containsKey(expression.getOperation())) {
                        STRATEGIES.get(expression.getOperation()).parse(expression, ddlConfig, originalFieldMappings);
                    }
                }
            }
        } catch (JSQLParserException e) {
            logger.error(e.getMessage(), e);
        }
        return ddlConfig;
    }

    @Override
    public List<FieldMapping> refreshFiledMappings(List<FieldMapping> originalFieldMappings, MetaInfo originMetaInfo, MetaInfo targetMetaInfo, DDLConfig targetDDLConfig) {
        List<FieldMapping> newTargetMappingList = new LinkedList<>();
        //处理映射关系
        for (FieldMapping fieldMapping : originalFieldMappings) {
            String fieldSourceName = fieldMapping.getSource().getName();
            String filedTargetName = fieldMapping.getTarget().getName();
            //找到更改的源表的名称，也就是找到了对应的映射关系，这样就可以从源表找到更改后的名称进行对应，
            if (fieldSourceName.equals(targetDDLConfig.getSourceColumnName())) {
                // 说明字段名没有改变，只是改变了属性
                if (targetDDLConfig.getDdlOperationEnum() == DDLOperationEnum.ALTER_MODIFY) {
                    Field source = originMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), fieldSourceName)).findFirst().get();
                    Field target = targetMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), filedTargetName)).findFirst().get();
                    //替换
                    newTargetMappingList.add(new FieldMapping(source, target));
                    continue;
                } else if (targetDDLConfig.getDdlOperationEnum() == DDLOperationEnum.ALTER_CHANGE) {
                    Field source = originMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), targetDDLConfig.getChangedColumnName())).findFirst().get();
                    Field target = targetMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), targetDDLConfig.getChangedColumnName())).findFirst().get();
                    //替换
                    newTargetMappingList.add(new FieldMapping(source, target));
                    continue;
                }
            }
            newTargetMappingList.add(fieldMapping);
        }

        if (DDLOperationEnum.ALTER_ADD == targetDDLConfig.getDdlOperationEnum()) {
            //处理新增的映射关系
            List<Field> addFields = targetDDLConfig.getAddFields();
            for (Field field : addFields) {
                Field source = originMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), field.getName())).findFirst().get();
                Field target = targetMetaInfo.getColumn().stream().filter(x -> StringUtil.equals(x.getName(), field.getName())).findFirst().get();
                newTargetMappingList.add(new FieldMapping(source, target));
            }
        }

        if (DDLOperationEnum.ALTER_DROP == targetDDLConfig.getDdlOperationEnum()) {
            //处理删除字段的映射关系
            List<Field> removeFields = targetDDLConfig.getRemoveFields();
            for (Field field : removeFields) {
                newTargetMappingList.removeIf(x -> StringUtil.equals(x.getSource().getName(), field.getName()));
            }
        }
        return newTargetMappingList;
    }

}