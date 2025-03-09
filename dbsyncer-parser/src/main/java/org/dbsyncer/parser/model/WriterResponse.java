package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.flush.BufferResponse;

import java.util.LinkedList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/27 18:11
 */
public class WriterResponse extends AbstractWriter implements BufferResponse {

    private final List<List<Object>> dataList = new LinkedList<>();

    private transient boolean isMerged;

    @Override
    public int getTaskSize() {
        return dataList.size();
    }

    @Override
    public String getSuffixName() {
        return StringUtil.HORIZONTAL.concat(getEvent());
    }

    public void addData(List<Object> data) {
        dataList.add(data);
    }

    public List<List<Object>> getDataList() {
        return dataList;
    }

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean merged) {
        isMerged = merged;
    }
}