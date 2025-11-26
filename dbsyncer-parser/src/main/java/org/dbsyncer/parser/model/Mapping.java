package org.dbsyncer.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.SyncPhaseEnum;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;

import java.time.Instant;
import java.util.List;

/**
 * 驱动映射关系
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 13:19
 */
public class Mapping extends AbstractConfigModel {

    public Mapping() {
        super.setType(ConfigConstant.MAPPING);
    }

    // 数据源连接器ID
    private String sourceConnectorId;

    // 目标源连接器ID
    private String targetConnectorId;

    // 数据源字段
    private List<Field> sourceColumn;

    // 目标源字段
    private List<Field> targetColumn;

    /**
     * 同步方式
     *
     * @see ModelEnum
     */
    private String model;

    // 监听配置
    private ListenerConfig listener;

    // 元信息ID
    private String metaId;

    // 批量读取
    private int readNum = 10000;

    // 单次写入
    private int batchNum = 1000;

    // 线程数
    private int threadNum = 10;

    // 覆盖写入
    private boolean forceUpdate = true;

    // 数据校验
    private boolean compareData = true;

    // 数据订正
    private boolean recoverData = true;

    @JsonIgnore
    public ProfileComponent profileComponent;


    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public void setSourceConnectorId(String sourceConnectorId) {
        this.sourceConnectorId = sourceConnectorId;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public void setTargetConnectorId(String targetConnectorId) {
        this.targetConnectorId = targetConnectorId;
    }

    public List<Field> getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(List<Field> sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public List<Field> getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(List<Field> targetColumn) {
        this.targetColumn = targetColumn;
    }

    public String getModel() {
        return model;
    }

    public Mapping setModel(String model) {
        this.model = model;
        return this;
    }

    public ListenerConfig getListener() {
        return listener;
    }

    public Mapping setListener(ListenerConfig listener) {
        this.listener = listener;
        return this;
    }

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public int getReadNum() {
        return readNum;
    }

    public void setReadNum(int readNum) {
        this.readNum = readNum;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public boolean isForceUpdate() {
        return forceUpdate;
    }

    public void setForceUpdate(boolean forceUpdate) {
        this.forceUpdate = forceUpdate;
    }

    public boolean isCompareData() {
        return compareData;
    }

    public void setCompareData(boolean compareData) {
        this.compareData = compareData;
    }

    public boolean isRecoverData() {
        return recoverData;
    }

    public void setRecoverData(boolean recoverData) {
        this.recoverData = recoverData;
    }

    @JsonIgnore
    public Meta getMeta() {
        return profileComponent.getMeta(this.metaId);
    }

    @JsonIgnore
    public void resetMetaState() throws Exception {
        Meta meta = profileComponent.getMeta(getMetaId());
        meta.resetState();
    }

    @JsonIgnore
    public void updateMata(String metaSnapshot) throws Exception {
        Meta meta = profileComponent.getMeta(getMetaId());
        meta.updateSnapshot(metaSnapshot);
        profileComponent.editConfigModel(this);

    }

    @JsonIgnore
    public Mapping copy(SnowflakeIdWorker snowflakeIdWorker) throws Exception {
        String json = JsonUtil.objToJson(this);
        Mapping newMapping = JsonUtil.jsonToObj(json, Mapping.class);
        newMapping.profileComponent = profileComponent;
        newMapping.setName(this.getName() + "(复制)");
        String newId = String.valueOf(snowflakeIdWorker.nextId());
        newMapping.setId(newId);
        newMapping.setUpdateTime(Instant.now().toEpochMilli());
        // meta
        Meta.create(newMapping, snowflakeIdWorker, profileComponent);
        profileComponent.addConfigModel(newMapping);

        // 复制映射表关系
        List<TableGroup> groupList = profileComponent.getTableGroupAll(this.getId());
        if (CollectionUtils.isEmpty(groupList)) {
            return newMapping;
        }
        groupList.forEach(tableGroup -> {
            try {
                tableGroup.copy(newId, snowflakeIdWorker);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return newMapping;
    }
}