package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.TableGroup;

import java.util.Map;

public class MessageVo {

    private TableGroup tableGroup;

    private Map row;

    public TableGroup getTableGroup() {
        return tableGroup;
    }

    public void setTableGroup(TableGroup tableGroup) {
        this.tableGroup = tableGroup;
    }

    public Map getRow() {
        return row;
    }

    public void setRow(Map row) {
        this.row = row;
    }
}
