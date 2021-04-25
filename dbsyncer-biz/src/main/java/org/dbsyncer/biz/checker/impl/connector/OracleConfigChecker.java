package org.dbsyncer.biz.checker.impl.connector;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * <p>1、增量同步时，目标源必须有一个主键字段用于接收ROW_ID值。</p>
 * <p>2、全局可配置目标源ROW_ID字段名称，默认为ROW_ID_NAME。 </p>
 * <p>3、如果配置了接收字段，添加字段映射关系[ROW_ID_NAME] > [ROW_ID_NAME]，并将ROW_ID_NAME字段设置为目标源的唯一主键。</p>
 * <p>4、全量同步时，ROW_ID_NAME参数非必须。</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class OracleConfigChecker extends AbstractDataBaseConfigChecker {

    /**
     * 默认ROWID列名称
     */
    private static final String ROW_ID_NAME = "ORACLE_ROW_ID";
    private static final String ROW_ID      = "ROWIDTOCHAR(ROWID) as " + ROW_ID_NAME;

    @Autowired
    private Manager manager;

    @Override
    public void dealIncrementStrategy(Mapping mapping, TableGroup tableGroup) {
        // TODO 模拟测试
        Map<String, String> params = mapping.getParams();
        mapping.getParams().put(ROW_ID_NAME, "RID");

        // 没有定义目标源ROW_ID字段
        if (CollectionUtils.isEmpty(mapping.getParams()) || !params.containsKey(ROW_ID_NAME)) {
            return;
        }

        // 检查是否更新
        String targetRowIdName = params.get(ROW_ID_NAME);
        for (FieldMapping m : tableGroup.getFieldMapping()) {
            if (StringUtils.equals(m.getSource().getName(), ROW_ID)) {
                m.getTarget().setName(targetRowIdName);
                m.getTarget().setPk(true);
                return;
            }
        }

        List<Field> sourceColumn = tableGroup.getSourceTable().getColumn();
        Field sourceField = new Field(ROW_ID, "VARCHAR2", 12, false, true);
        sourceColumn.add(0, sourceField);

        List<Field> targetColumn = tableGroup.getTargetTable().getColumn();
        targetColumn.parallelStream().forEach(f -> f.setPk(false));
        Field targetField = new Field(targetRowIdName, "VARCHAR2", 12, true, false);
        targetColumn.add(0, targetField);

        tableGroup.getFieldMapping().add(0, new FieldMapping(sourceField, targetField));
    }

}