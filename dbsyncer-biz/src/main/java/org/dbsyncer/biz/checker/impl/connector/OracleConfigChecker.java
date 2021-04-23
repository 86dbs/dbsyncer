package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.connector.config.Field;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.FieldMapping;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private Manager manager;

    @Override
    public void dealIncrementStrategy(Mapping mapping, TableGroup tableGroup) {
        /**
         * 1、增量同步时，目标源必须有一个主键字段用于接收ROW_ID值。
         * 2、全局可配置目标源ROW_ID字段名称，默认为ROW_ID_NAME。
         * 3、如果配置了接收字段，添加字段映射关系[ROW_ID_NAME]> [ROW_ID_NAME]，并将ROW_ID_NAME字段设置为目标源的唯一主键。
         * 4、全量同步时，ROW_ID_NAME参数非必须。
         */
        // TODO mapping.enableRowId
        boolean enableRowId = false;
        if(!enableRowId){
            return;
        }

    }

    private Field getSourceField() {
        return new Field(ROW_ID_NAME, "VARCHAR2", 12, false, true);
    }
}