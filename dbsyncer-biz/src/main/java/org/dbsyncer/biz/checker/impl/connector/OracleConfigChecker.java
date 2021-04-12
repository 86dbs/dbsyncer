package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.connector.config.Field;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class OracleConfigChecker extends AbstractDataBaseConfigChecker {

    /**
     * 默认ROWID列名称
     */
    private static final String ROW_ID_NAME = "DBSYNCER_ROWID";

    @Override
    public void updateFields(Mapping mapping, TableGroup tableGroup, List<Field> column, boolean isSourceTable) {
        // TODO mapping.enableRowId
        boolean enableRowId = false;
        if(!enableRowId){
            return;
        }

        // TODO source   Oralce >> Oracle   Oralce >> Mysql   Mysql >> Oracle
        if (isSourceTable) {
            List<Field> list = new ArrayList<>();
            list.add(getSourceField());
            list.addAll(column);
            column.clear();
            column.addAll(list);
            return;
        }

        // Target
        // TODO mapping.rowIdName
        String rowIdName = "RID";
        // Oracle 默认加上ROWID名称，设置为唯一主键
        column.parallelStream().forEach(f -> f.setPk(false));
        Field targetField = new Field(rowIdName, "VARCHAR2", 12, true);
        List<Field> list = new ArrayList<>();
        list.add(targetField);
        list.addAll(column);
        column.clear();
        column.addAll(list);

        // 添加默认ROWID映射关系
        tableGroup.getFieldMapping().add(new FieldMapping(getSourceField(), targetField));
    }

    private Field getSourceField() {
        return new Field(ROW_ID_NAME, "VARCHAR2", 12, false, true);
    }
}