package org.dbsyncer.parser.model;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.listener.Listener;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * 驱动同步元信息
 * </p>
 *
 * <pre>
 *     全量同步: 存放分页数
 *     增量同步:定时>时间戳; 日志>binlogFileName/binlogPosition/主从节点信息等
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public class Meta extends ConfigModel {

    private String mappingId;
    /**
     * {@link MetaEnum}
     */
    private int state;
    private AtomicLong total;
    private AtomicLong success;
    private AtomicLong fail;
    private Map<String, String> snapshot;
    private long beginTime;
    private long endTime;
    @JSONField(serialize = false)
    private Task task;
    @JSONField(serialize = false)
    private transient Listener listener;

    // 驱动异常信息
    private String errorMessage = "";

    // ProfileComponent实例，用于保存Meta对象
    @JSONField(serialize = false)
    private transient ProfileComponent profileComponent;

    public Meta() {
        super.setType(ConfigConstant.META);
        super.setName(ConfigConstant.META);
        init();
    }

    public Meta(ProfileComponent profileComponent) {
        this();
        assert profileComponent != null;
        this.profileComponent = profileComponent;
    }

    /**
     * 还原状态
     */
    public void clear() {
        init();
    }

    private void init() {
        this.state = MetaEnum.READY.getCode();
        this.total = new AtomicLong(0);
        this.success = new AtomicLong(0);
        this.fail = new AtomicLong(0);
        this.snapshot = new HashMap<>();
        this.beginTime = 0L;
        this.endTime = 0L;
        // 初始化异常信息
        this.errorMessage = "";
    }

    public void setProfileComponent(ProfileComponent profileComponent) {
        if (this.profileComponent != null) {
            return;
        }
        this.profileComponent = profileComponent;
    }

    public String getMappingId() {
        return mappingId;
    }

    public void setMappingId(String mappingId) {
        this.mappingId = mappingId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
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

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Listener getListener() {
        return listener;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * 保存状态到持久化存储
     *
     * @param state        新的状态
     * @param errorMessage 异常信息（可选）
     */
    public void saveState(MetaEnum state, String errorMessage) {
        this.state = state.getCode();
        this.errorMessage = errorMessage;
        this.setUpdateTime(Instant.now().toEpochMilli());
        this.profileComponent.editConfigModel(this);
    }

    /**
     * 保存状态到持久化存储（仅状态）
     *
     * @param state 新的状态
     */
    public void saveState(MetaEnum state) {
        saveState(state, "");
    }


    /**
     * 重置状态到初始状态（不重置计数数据）
     */
    public void resetState() {
        saveState(MetaEnum.READY, "");
    }

    /**
     * 检查是否处于异常状态
     *
     * @return 如果处于异常状态返回true，否则返回false
     */
    @JSONField(serialize = false)
    public boolean isError() {
        return this.state == MetaEnum.ERROR.getCode();
    }

    /**
     * 检查是否处于运行状态
     *
     * @return 如果处于运行状态返回true，否则返回false
     */
    @JSONField(serialize = false)
    public boolean isRunning() {
        return MetaEnum.isRunning(this.state);
    }
}