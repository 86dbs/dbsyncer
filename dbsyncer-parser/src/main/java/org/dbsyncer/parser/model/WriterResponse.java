package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.flush.BufferResponse;
import org.dbsyncer.sdk.model.ChangedOffset;

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

    private transient Boolean isMerged;

    @Override
    public int getTaskSize() {
        return dataList.size();
    }

    @Override
    public String getSuffixName() {
        return StringUtil.SYMBOL.concat(getEvent());
    }

    public void addData(Map data) {
        dataList.add(data);
    }

    public void addChangedOffset(ChangedOffset changedOffset) {
        offsetList.add(changedOffset);
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public List<ChangedOffset> getOffsetList() {
        return offsetList;
    }

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean merged) {
        isMerged = merged;
    }
}