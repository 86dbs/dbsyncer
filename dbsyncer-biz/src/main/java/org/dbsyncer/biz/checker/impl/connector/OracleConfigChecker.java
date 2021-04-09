package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.connector.config.Field;
import org.dbsyncer.parser.model.Mapping;
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

    @Override
    public void updateFields(Mapping mapping, List<Field> column, boolean isSourceTable) {
        String rowId = "DBSYNCER_ROWID";

        // TODO source   Oralce >> Oracle   Oralce >> Mysql   Mysql >> Oracle
        if(isSourceTable) {
            String rowIdName = rowId;
            List<Field> list = new ArrayList<>();
            list.add(new Field(rowIdName, "VARCHAR2",12, CollectionUtils.isEmpty(column)));
            list.addAll(column);
            column.clear();
            column.addAll(list);
            return;
        }

        // mapping 配置全局目标源ROWID名称
        // Target
        // Oracle 默认加上ROWID名称，设置为唯一主键
        String rowIdName = rowId;
        List<Field> list = new ArrayList<>();
        list.add(new Field(rowIdName, "VARCHAR2",12, true));
        column.parallelStream().forEach(f -> f.setPk(false));
        list.addAll(column);
        column.clear();
        column.addAll(list);
    }
}