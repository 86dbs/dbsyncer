package org.dbsyncer.parser.model;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务执行结果(dbsyncer_meta)。
 * <p>主键 {@code id} 为雪花，与业务实体 ID 解耦。
 * <p>关联键 {@code taskId}：任务级（{@code isTaskDetail=0}）为任务/Mapping ID；
 * 表级（{@code isTaskDetail=1}）为 {@code table_group.id}。

 * <p>明细分表 {@code dbsyncer_task_detail_{taskId}} 分片使用任务级 Meta 的 taskId，不是 Meta 主键。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public class Meta extends ConfigModel {

    /**
     * 关联 ID：任务级为任务/Mapping ID；表级为 table_group.id
     */
    private String taskId;

    /**
     * {@link CommonTaskStatusEnum}
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
        this.state = CommonTaskStatusEnum.READY.getCode();
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
