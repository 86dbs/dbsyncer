package org.dbsyncer.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.enums.SyncPhaseEnum;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.listener.Listener;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
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
    private Map<String, String> snapshot;   // 仅保存增量同步的 cursor 信息，因为增量是库级别的
    private long beginTime;
    private long endTime;
    @JsonIgnore
    private transient Listener listener;

    // 驱动异常信息
    private String errorMessage = "";

    // ProfileComponent实例，用于保存Meta对象
    @JsonIgnore
    private transient ProfileComponent profileComponent;

    // 混合同步阶段
    private SyncPhaseEnum syncPhase = SyncPhaseEnum.FULL;

    // 简化的受保护字段名常量
    @JsonIgnore
    private static final String PROTECTED_INCREMENT_INFO = "_protected_increment_info";

    // 回调函数支持
    @JsonIgnore
    private transient Runnable phaseHandler;

    public Meta() {
        super.setType(ConfigConstant.META);
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
    public void clear() throws Exception {
        init();
        profileComponent.editConfigModel(this);
    }

    public void clear(String model) throws Exception {
        init();
        // 为计数设置阶段
        if (model.equals(ModelEnum.INCREMENT.getCode())) {
            this.setSyncPhase(SyncPhaseEnum.INCREMENTAL);
        }
        profileComponent.editConfigModel(this);
    }


    private void init() {
        long now = Instant.now().toEpochMilli();
        this.state = MetaEnum.READY.getCode();
        this.total = new AtomicLong(0);
        this.success = new AtomicLong(0);
        this.fail = new AtomicLong(0);
        this.snapshot = new HashMap<>();
        this.beginTime = now;
        this.setUpdateTime(now);
        this.endTime = now;
        // 初始化异常信息
        this.errorMessage = "";
        // 初始化混合同步阶段
        this.syncPhase = SyncPhaseEnum.FULL;
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

    /**
     * 更新全量同步总条数
     */
    public void updateFullTotal() throws Exception {
        // 全量同步
        if (SyncPhaseEnum.FULL != getSyncPhase()) {
            return;
        }
        // 统计tableGroup总条数
        AtomicLong count = new AtomicLong(0);
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(this.getMappingId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            for (TableGroup g : groupAll) {
                count.getAndAdd(g.getSourceTable().getCount());
            }
        }
        total = count;
        setUpdateTime(Instant.now().toEpochMilli());
        profileComponent.editConfigModel(this);
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

    public void updateSnapshot(String metaSnapshot) throws Exception {
        if (StringUtil.isNotBlank(metaSnapshot)) {
            @SuppressWarnings("unchecked")
        Map<String, String> snapshot = (Map<String, String>) (Map<?, ?>) JsonUtil.parseMap(metaSnapshot);
            if (!CollectionUtils.isEmpty(snapshot)) {
                this.snapshot = snapshot;
                setUpdateTime(Instant.now().toEpochMilli());
                profileComponent.editConfigModel(this);
            }
        }
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

    // 新增的实例方法
    public SyncPhaseEnum getSyncPhase() {
        return syncPhase;
    }

    public void setSyncPhase(SyncPhaseEnum syncPhase) {
        this.syncPhase = syncPhase;
    }

    public void updateSyncPhase(SyncPhaseEnum phase) throws Exception {
        this.syncPhase = phase;
        // 假设 Meta 类提供了 save 或类似方法来持久化自身
        if (this.profileComponent != null) {
            this.profileComponent.editConfigModel(this);
        }
    }

    // 回调函数支持
    public void setPhaseHandler(Runnable handler) {
        this.phaseHandler = handler;
    }

    public Runnable getPhaseHandler() {
        return this.phaseHandler;
    }

    // 检查是否已记录增量起始点
    @JsonIgnore
    public boolean isIncrementStartPointRecorded() {
        return this.snapshot.containsKey(PROTECTED_INCREMENT_INFO);
    }

    // 记录增量起始点到受保护字段
    public void recordIncrementStartPoint(Map<String, String> position) throws Exception {
        // 使用JsonUtil序列化position
        String incrementInfoJson = JsonUtil.objToJson(position);
        this.snapshot.put(PROTECTED_INCREMENT_INFO, incrementInfoJson);

        // 保存到持久化存储
        this.profileComponent.editConfigModel(this);
    }

    // 恢复受保护的增量起始点到正常字段
    public void restoreProtectedIncrementStartPoint() throws Exception {
        String incrementInfoJson = this.snapshot.get(PROTECTED_INCREMENT_INFO);
        if (StringUtil.isBlank(incrementInfoJson)) {
            // 已经开始了增量同步
            return;
        }
        this.snapshot.remove(PROTECTED_INCREMENT_INFO);

        // 将 incrementInfoJson 反序列化为 Map<String, String>
        @SuppressWarnings("unchecked")
        Map<String, String> incrementInfo = (Map<String, String>) (Map<?, ?>) JsonUtil.parseMap(incrementInfoJson);
        this.snapshot.putAll(incrementInfo);

        this.profileComponent.editConfigModel(this);
    }

    /**
     * 保存状态到持久化存储
     *
     * @param state        新的状态
     * @param errorMessage 异常信息（可选）
     */
    public void saveState(MetaEnum state, String errorMessage) throws Exception {
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
    public void saveState(MetaEnum state) throws Exception {
        saveState(state, "");
    }

    /**
     * 重置状态到初始状态（不重置计数数据）
     */
    public void resetState() throws Exception {
        saveState(MetaEnum.READY, "");
    }

    /**
     * 检查是否处于异常状态
     *
     * @return 如果处于异常状态返回true，否则返回false
     */
    @JsonIgnore
    public boolean isError() {
        return this.state == MetaEnum.ERROR.getCode();
    }

    /**
     * 检查是否处于运行状态
     *
     * @return 如果处于运行状态返回true，否则返回false
     */
    @JsonIgnore
    public boolean isRunning() {
        return MetaEnum.isRunning(this.state);
    }

    @JsonIgnore
    public static Meta create(Mapping mapping, SnowflakeIdWorker snowflakeIdWorker, ProfileComponent profileComponent) throws Exception {
        Meta meta = new Meta(profileComponent);
        String newId = String.valueOf(snowflakeIdWorker.nextId());
        meta.setId(newId);
        meta.setCreateTime(Instant.now().toEpochMilli());
        meta.setMappingId(mapping.getId());
        meta.setName(mapping.getId());
        mapping.setMetaId(newId);
        profileComponent.addConfigModel(meta);
        return meta;
    }
}