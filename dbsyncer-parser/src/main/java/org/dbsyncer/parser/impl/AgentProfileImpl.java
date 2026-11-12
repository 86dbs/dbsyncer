/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.AgentProfile;
import org.dbsyncer.parser.model.AgentConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link AgentProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
@Component
public class AgentProfileImpl implements AgentProfile {

    private static final String TABLE = "dbsyncer_agent";

    private static final String COLUMNS =
            "ID AS id, CREATE_TIME AS createTime, UPDATE_TIME AS updateTime, NAME AS name,"
                    + " LISTEN_PORT AS listenPort, AGENT_SECRET AS agentSecret, THREAD_NUM AS threadNum,"
                    + " CAPTURE_INTERVAL_MS AS captureIntervalMs, CHECKPOINT AS checkpoint,"
                    + " PUBLIC_KEY AS publicKey, PRIVATE_KEY AS privateKey, `JSON` AS json, STATE AS state";

    @Resource
    private StorageService storageService;

    @Override
    public AgentConfig get(String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        List<AgentConfig> list = storageService.query(
                "SELECT " + COLUMNS + " FROM " + TABLE + " WHERE ID=? LIMIT 1",
                new Object[]{id}, AgentConfig.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public List<AgentConfig> listAll() {
        List<AgentConfig> list = storageService.query(
                "SELECT " + COLUMNS + " FROM " + TABLE + " ORDER BY UPDATE_TIME DESC",
                new Object[]{}, AgentConfig.class);
        return list == null ? Collections.emptyList() : list;
    }

    @Override
    public void add(AgentConfig config) {
        long now = System.currentTimeMillis();
        if (StringUtil.isBlank(config.getId())) {
            config.setId(UUIDUtil.getUUID());
        }
        if (config.getCreateTime() == null) {
            config.setCreateTime(now);
        }
        if (config.getState() == null) {
            config.setState(0);
        }
        if (config.getThreadNum() == null) {
            config.setThreadNum(4);
        }
        if (config.getCaptureIntervalMs() == null) {
            config.setCaptureIntervalMs(1000);
        }
        config.setUpdateTime(now);
        storageService.add(StorageEnum.AGENT, toMap(config));
    }

    @Override
    public void edit(AgentConfig config) {
        config.setUpdateTime(System.currentTimeMillis());
        storageService.edit(StorageEnum.AGENT, toMap(config));
    }

    @Override
    public void remove(String id) {
        if (StringUtil.isBlank(id)) {
            return;
        }
        storageService.remove(StorageEnum.AGENT, id);
    }

    private Map<String, Object> toMap(AgentConfig config) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, config.getId());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, config.getName());
        params.put(ConfigConstant.AGENT_LISTEN_PORT, config.getListenPort());
        params.put(ConfigConstant.AGENT_SECRET, config.getAgentSecret());
        params.put(ConfigConstant.AGENT_THREAD_NUM, config.getThreadNum());
        params.put(ConfigConstant.AGENT_CAPTURE_INTERVAL_MS, config.getCaptureIntervalMs());
        params.put(ConfigConstant.AGENT_CHECKPOINT, config.getCheckpoint());
        params.put(ConfigConstant.AGENT_PUBLIC_KEY, config.getPublicKey());
        params.put(ConfigConstant.AGENT_PRIVATE_KEY, config.getPrivateKey());
        params.put(ConfigConstant.CONFIG_MODEL_JSON, config.getJson());
        params.put(ConfigConstant.AGENT_STATE, config.getState());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, config.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, config.getUpdateTime());
        return params;
    }
}
