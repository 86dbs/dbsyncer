package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.flush.BufferResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 18:11
 */
public class WriterResponse extends AbstractWriter implements BufferResponse {

    private List<Map> dataList = new LinkedList<>();

    private List<ChangedOffset> offsetList = new LinkedList<>();

    private String sql;

    private boolean isMerged;

    @Override
    public int getTaskSize() {
        return dataList.size();
    }

    @Override
    public String getSuffixName() {
        return StringUtil.SYMBOL.concat(getEvent());
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public List<ChangedOffset> getOffsetList() {
        return offsetList;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean merged) {
        isMerged = merged;
    }
}