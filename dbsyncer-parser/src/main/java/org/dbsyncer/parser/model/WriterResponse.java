package org.dbsyncer.parser.model;

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
    private List<String> messageIds = new LinkedList<>();

    private boolean isMerged;

    @Override
    public int getTaskSize() {
        return dataList.size();
    }

    public List<Map> getDataList() {
        return dataList;
    }

    public List<String> getMessageIds() {
        return messageIds;
    }

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean merged) {
        isMerged = merged;
    }
}