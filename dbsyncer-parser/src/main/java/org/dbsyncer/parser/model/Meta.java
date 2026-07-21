package org.dbsyncer.parser.model;

import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.sdk.constant.ConfigConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务执行结果(dbsyncer_meta)。
 * <p>任务级：{@code isTaskDetail=0}，{@code taskId}=任务ID；
 * 明细级：{@code isTaskDetail=1}，校验/迁移时 {@code taskId}=task_detail.id，同步时 {@code taskId}=table_group.id。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public class Meta extends ConfigModel {

    /**
     * 关联ID：任务级为 taskId；明细级为 task_detail.id 或 table_group.id
     */
    private String taskId;

    /**
     * {@link MetaEnum}
     */
    private int state;

    /**
     * 是否任务明细：0-任务级 1-明细级
     */
    private int isTaskDetail;

    private AtomicLong total;
    private AtomicLong success;
    private AtomicLong fail;
    private AtomicLong diff;
    private AtomicLong fixed;
    private Map<String, String> snapshot;
    private long beginTime;
    private long endTime;

    public Meta() {
        super.setType(ConfigConstant.META);
        super.setName(ConfigConstant.META);
        init();
    }

    /**
     * 还原状态
     */
    public void clear() {
        init();
    }

    private void init() {
        this.state = MetaEnum.READY.getCode();
        this.isTaskDetail = 0;
        this.total = new AtomicLong(0);
        this.success = new AtomicLong(0);
        this.fail = new AtomicLong(0);
        this.diff = new AtomicLong(0);
        this.fixed = new AtomicLong(0);
        this.snapshot = new HashMap<>();
        this.beginTime = 0L;
        this.endTime = 0L;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @deprecated 使用 {@link #getTaskId()}
     */
    @Deprecated
    public String getMappingId() {
        return taskId;
    }

    /**
     * @deprecated 使用 {@link #setTaskId(String)}
     */
    @Deprecated
    public void setMappingId(String mappingId) {
        this.taskId = mappingId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public int getIsTaskDetail() {
        return isTaskDetail;
    }

    public void setIsTaskDetail(int isTaskDetail) {
        this.isTaskDetail = isTaskDetail;
    }

    public boolean isTaskDetail() {
        return isTaskDetail == 1;
    }

    public AtomicLong getTotal() {
        return total;
    }

    public void setTotal(AtomicLong total) {
        this.total = total;
    }

    public AtomicLong getSuccess() {
        return success;
    }

    public void setSuccess(AtomicLong success) {
        this.success = success;
    }

    public AtomicLong getFail() {
        return fail;
    }

    public void setFail(AtomicLong fail) {
        this.fail = fail;
    }

    public AtomicLong getDiff() {
        return diff;
    }

    public void setDiff(AtomicLong diff) {
        this.diff = diff;
    }

    public AtomicLong getFixed() {
        return fixed;
    }

    public void setFixed(AtomicLong fixed) {
        this.fixed = fixed;
    }

    public Map<String, String> getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Map<String, String> snapshot) {
        this.snapshot = snapshot;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
}
