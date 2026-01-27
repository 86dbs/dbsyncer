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
    private List<String> columnNames;  // CDC 捕获的列名列表（按数据顺序）

    private transient boolean isMerged;
    
    /**
     * 是否为重试操作（用于防止重试失败时再次写入错误队列）
     */
    private transient boolean isRetry = false;

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

    /**
     * 获取 CDC 捕获的列名列表（按数据顺序）
     * 
     * @return 列名列表，如果为 null 表示使用 TableGroup 的字段信息
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * 设置 CDC 捕获的列名列表
     */
    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * 是否为重试操作
     */
    public boolean isRetry() {
        return isRetry;
    }

    /**
     * 设置是否为重试操作
     */
    public void setRetry(boolean retry) {
        isRetry = retry;
    }
}