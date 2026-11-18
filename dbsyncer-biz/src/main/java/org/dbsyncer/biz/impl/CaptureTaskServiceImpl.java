/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.CaptureTaskService;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.RSAUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.CaptureTaskRelProfile;
import org.dbsyncer.parser.TaskMetaProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.CaptureTaskRel;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.CaptureLocalTask;
import org.dbsyncer.sdk.model.CaptureRemoteTask;
import org.dbsyncer.sdk.spi.CaptureAgentService;
import org.dbsyncer.sdk.util.AgentOffsetUtil;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link CaptureTaskService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
@Service
public class CaptureTaskServiceImpl implements CaptureTaskService {

    @Resource
    private TaskProfile taskProfile;
    @Resource
    private TaskMetaProfile taskMetaProfile;
    @Resource
    private CaptureTaskRelProfile captureTaskRelProfile;
    @Resource
    private CaptureAgentService captureAgentService;
    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public List<Map<String, Object>> list(String type) {
        List<Map<String, Object>> result = new ArrayList<>();
        String normalized = StringUtil.trim(type);
        boolean all = StringUtil.isBlank(normalized);
        if (all || ConfigConstant.CAPTURE_LOCAL.equals(normalized)) {
            collect(result, taskProfile.queryTasks(CaptureLocalTask.class, 1, Integer.MAX_VALUE, null, null).getData());
        }
        if (all || ConfigConstant.CAPTURE_REMOTE.equals(normalized)) {
            collect(result, taskProfile.queryTasks(CaptureRemoteTask.class, 1, Integer.MAX_VALUE, null, null).getData());
        }
        result.sort(Comparator.comparing(
                (Map<String, Object> m) -> NumberUtil.toLong(String.valueOf(m.get("updateTime")), 0L)).reversed());
        return result;
    }

    @Override
    public Map<String, Object> get(String id) {
        CaptureLocalTask local = taskProfile.getTask(id, CaptureLocalTask.class);
        if (local != null && ConfigConstant.CAPTURE_LOCAL.equals(local.getType())) {
            return toLocalView(local, true);
        }
        CaptureRemoteTask remote = taskProfile.getTask(id, CaptureRemoteTask.class);
        if (remote != null && ConfigConstant.CAPTURE_REMOTE.equals(remote.getType())) {
            return toRemoteView(remote);
        }
        return null;
    }

    @Override
    public String add(Map<String, String> params) {
        String type = StringUtil.trim(params.get(ConfigConstant.CONFIG_MODEL_TYPE));
        Assert.hasText(type, "任务类型不能为空");
        RsaVersion keys = RSAUtil.createKeys(2048);
        if (ConfigConstant.CAPTURE_LOCAL.equals(type)) {
            CaptureLocalTask task = buildLocal(params, null);
            task.setPublicKey(keys.getPublicKey());
            task.setPrivateKey(keys.getPrivateKey());
            ensureTimestamps(task);
            String id = taskProfile.addTask(task);
            addTaskMeta(id);
            return id;
        }
        if (ConfigConstant.CAPTURE_REMOTE.equals(type)) {
            CaptureRemoteTask task = buildRemote(params, null);
            task.setPublicKey(keys.getPublicKey());
            task.setPrivateKey(keys.getPrivateKey());
            ensureTimestamps(task);
            String id = taskProfile.addTask(task);
            addTaskMeta(id);
            return id;
        }
        throw new BizException("不支持的采集任务类型: " + type);
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        Assert.hasText(id, "ID不能为空");
        assertNotRunning(id);
        CaptureLocalTask local = taskProfile.getTask(id, CaptureLocalTask.class);
        if (local != null && ConfigConstant.CAPTURE_LOCAL.equals(local.getType())) {
            CaptureLocalTask task = buildLocal(params, local);
            ensureTimestamps(task);
            return taskProfile.updateTask(task);
        }
        CaptureRemoteTask remote = taskProfile.getTask(id, CaptureRemoteTask.class);
        if (remote != null && ConfigConstant.CAPTURE_REMOTE.equals(remote.getType())) {
            CaptureRemoteTask task = buildRemote(params, remote);
            ensureTimestamps(task);
            return taskProfile.updateTask(task);
        }
        throw new BizException("采集任务不存在");
    }

    @Override
    public void remove(String id) {
        Assert.hasText(id, "ID不能为空");
        assertNotRunning(id);
        CaptureRemoteTask remote = taskProfile.getTask(id, CaptureRemoteTask.class);
        if (remote != null && ConfigConstant.CAPTURE_REMOTE.equals(remote.getType())) {
            captureTaskRelProfile.removeByCaptureTaskId(id);
        }
        taskMetaProfile.removeMeta(id);
        taskProfile.deleteTask(id);
    }

    @Override
    public String generateSecret() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private void collect(List<Map<String, Object>> result, Collection<?> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        for (Object task : tasks) {
            if (task instanceof CaptureLocalTask) {
                result.add(toLocalView((CaptureLocalTask) task, false));
            } else if (task instanceof CaptureRemoteTask) {
                result.add(toRemoteView((CaptureRemoteTask) task));
            }
        }
    }

    private void assertNotRunning(String id) {
        Meta meta = taskMetaProfile.getMeta(id);
        if (meta != null && CommonTaskStatusEnum.isRunning(meta.getState())) {
            throw new BizException("运行中的采集任务不能操作，请先停止");
        }
    }

    private void addTaskMeta(String taskId) {
        Meta meta = new Meta();
        meta.setId(String.valueOf(snowflakeIdWorker.nextId()));
        meta.setTaskId(taskId);
        meta.setIsTaskDetail(TaskLevelEnum.TASK.getCode());
        meta.setState(CommonTaskStatusEnum.READY.getCode());
        long now = Instant.now().toEpochMilli();
        meta.setCreateTime(now);
        meta.setUpdateTime(now);
        taskMetaProfile.addMeta(meta);
    }

    private void ensureTimestamps(org.dbsyncer.common.model.ConfigModel task) {
        long now = Instant.now().toEpochMilli();
        if (task.getCreateTime() == null) {
            task.setCreateTime(now);
        }
        task.setUpdateTime(now);
    }

    private CaptureLocalTask buildLocal(Map<String, String> params, CaptureLocalTask existing) {
        String name = StringUtil.trim(params.get(ConfigConstant.CONFIG_MODEL_NAME));
        Assert.hasText(name, "名称不能为空");
        int listenPort = NumberUtil.toInt(params.get("listenPort"), 0);
        Assert.isTrue(listenPort > 0 && listenPort <= 65535, "监听端口不合法");
        String agentSecret = StringUtil.trim(params.get("agentSecret"));
        Assert.hasText(agentSecret, "密钥不能为空");
        String jdbcUrl = StringUtil.trim(params.get("jdbcUrl"));
        String username = StringUtil.trim(params.get("username"));
        Assert.hasText(jdbcUrl, "JDBC URL不能为空");
        Assert.hasText(username, "用户名不能为空");

        String password = params.get("password");
        String connectorType = params.get("connectorType");
        if (existing != null && StringUtil.isBlank(password)) {
            password = existing.getPassword();
        }
        Assert.hasText(password, "密码不能为空");

        CaptureLocalTask task = existing == null ? new CaptureLocalTask() : existing;
        if (existing == null && StringUtil.isNotBlank(params.get("id"))) {
            task.setId(params.get("id"));
        }
        task.setName(name);
        task.setConnectorType(connectorType);
        task.setListenPort(listenPort);
        task.setAgentSecret(agentSecret);
        task.setThreadNum(NumberUtil.toInt(params.get("threadNum"), 4));
        task.setCaptureIntervalMs(NumberUtil.toInt(params.get("captureIntervalMs"), 1000));
        String checkpoint = StringUtil.trim(params.get("checkpoint"));
        task.setCheckpoint(StringUtil.isBlank(checkpoint) ? "tail.ckpt" : checkpoint);
        task.setJdbcUrl(jdbcUrl);
        task.setUsername(username);
        task.setPassword(password);
        return task;
    }

    private CaptureRemoteTask buildRemote(Map<String, String> params, CaptureRemoteTask existing) {
        String name = StringUtil.trim(params.get(ConfigConstant.CONFIG_MODEL_NAME));
        Assert.hasText(name, "名称不能为空");
        String agentHost = StringUtil.trim(params.get("agentHost"));
        Assert.hasText(agentHost, "Agent主机不能为空");
        int agentPort = NumberUtil.toInt(params.get("agentPort"), 0);
        Assert.isTrue(agentPort > 0 && agentPort <= 65535, "Agent端口不合法");
        String agentSecret = StringUtil.trim(params.get("agentSecret"));
        Assert.hasText(agentSecret, "密钥不能为空");

        CaptureRemoteTask task = existing == null ? new CaptureRemoteTask() : existing;
        if (existing == null && StringUtil.isNotBlank(params.get("id"))) {
            task.setId(params.get("id"));
        }
        task.setName(name);
        String connectorType = StringUtil.trim(params.get("connectorType"));
        task.setConnectorType(StringUtil.isBlank(connectorType) ? "Oracle" : connectorType);
        task.setAgentHost(agentHost);
        task.setAgentPort(agentPort);
        task.setAgentSecret(agentSecret);
        task.setQueueCapacity(NumberUtil.toInt(params.get("queueCapacity"), 100000));
        return task;
    }

    private Map<String, Object> toLocalView(CaptureLocalTask task, boolean detail) {
        Map<String, Object> map = baseView(task.getId(), task.getName(), task.getType(),
                task.getCreateTime(), task.getUpdateTime());
        map.put("listenPort", task.getListenPort());
        map.put("agentSecret", task.getAgentSecret());
        map.put("threadNum", task.getThreadNum());
        map.put("captureIntervalMs", task.getCaptureIntervalMs());
        map.put("checkpoint", task.getCheckpoint());
        map.put("connectorType", StringUtil.isBlank(task.getConnectorType()) ? "Oracle" : task.getConnectorType());
        map.put("publicKey", task.getPublicKey());
        enrichLocalRuntime(map, task);
        if (detail) {
            map.put("jdbcUrl", task.getJdbcUrl());
            map.put("username", task.getUsername());
        }
        return map;
    }

    /**
     * 填充本地列表展示：推送队列占用、位点。
     */
    private void enrichLocalRuntime(Map<String, Object> map, CaptureLocalTask task) {
        Map<String, Object> runtime = captureAgentService.getLocalRuntimeInfo(task.getId());
        Integer queueSize = null;
        Integer queueCapacity = null;
        if (runtime != null) {
            if (runtime.get("queueSize") != null) {
                queueSize = NumberUtil.toInt(String.valueOf(runtime.get("queueSize")), -1);
                if (queueSize < 0) {
                    queueSize = null;
                }
            }
            if (runtime.get("queueCapacity") != null) {
                queueCapacity = NumberUtil.toInt(String.valueOf(runtime.get("queueCapacity")), -1);
                if (queueCapacity <= 0) {
                    queueCapacity = null;
                }
            }
            Object checkpoint = runtime.get("checkpoint");
            if (checkpoint instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> checkpointMap = (Map<String, Object>) checkpoint;
                map.put("checkpointDetail", checkpointMap);
                map.put("checkpointText", formatCheckpoint(checkpointMap));
            }
        }
        if (!map.containsKey("checkpointText")) {
            map.put("checkpointText", StringUtil.isBlank(task.getCheckpoint()) ? "-" : task.getCheckpoint());
        }
        map.put("queueSize", queueSize);
        map.put("queueCapacity", queueCapacity);
        if (queueCapacity != null) {
            map.put("queueCapacityText", (queueSize == null ? 0 : queueSize) + "/" + queueCapacity);
        } else {
            map.put("queueCapacityText", "-");
        }
    }

    private Map<String, Object> toRemoteView(CaptureRemoteTask task) {
        Map<String, Object> map = baseView(task.getId(), task.getName(), task.getType(),
                task.getCreateTime(), task.getUpdateTime());
        map.put("agentHost", task.getAgentHost());
        map.put("agentPort", task.getAgentPort());
        map.put("agentAddress", formatHostPort(task.getAgentHost(), task.getAgentPort()));
        map.put("agentSecret", task.getAgentSecret());
        map.put("connectorType", StringUtil.isBlank(task.getConnectorType()) ? "Oracle" : task.getConnectorType());
        map.put("queueCapacity", task.getQueueCapacity());
        map.put("publicKey", task.getPublicKey());
        map.put("schema", task.getSchema());
        enrichRemoteRelations(map, task);
        return map;
    }

    /**
     * 填充远端列表展示：关联同步任务、订阅 Schema、位点、运行队列占用。
     */
    private void enrichRemoteRelations(Map<String, Object> map, CaptureRemoteTask task) {
        List<CaptureTaskRel> rels = captureTaskRelProfile.listByCaptureTaskId(task.getId());
        List<String> mappingNames = new ArrayList<>();
        Set<String> schemas = new LinkedHashSet<>();
        if (StringUtil.isNotBlank(task.getSchema())) {
            String[] parts = StringUtil.split(task.getSchema(), StringUtil.COMMA);
            if (parts != null) {
                for (String part : parts) {
                    if (StringUtil.isNotBlank(part)) {
                        schemas.add(part.trim().toUpperCase());
                    }
                }
            }
        }
        Map<String, Object> minCheckpoint = null;
        if (!CollectionUtils.isEmpty(rels)) {
            for (CaptureTaskRel rel : rels) {
                if (rel == null || StringUtil.isBlank(rel.getTaskId())) {
                    continue;
                }
                Mapping mapping = taskProfile.getTask(rel.getTaskId(), Mapping.class);
                if (mapping != null) {
                    if (StringUtil.isNotBlank(mapping.getName())) {
                        mappingNames.add(mapping.getName());
                    }
                    if (StringUtil.isNotBlank(mapping.getSourceSchema())) {
                        schemas.add(mapping.getSourceSchema().trim().toUpperCase());
                    }
                } else {
                    mappingNames.add(rel.getTaskId());
                }
                Meta meta = taskMetaProfile.getMeta(rel.getTaskId());
                if (meta != null && !CollectionUtils.isEmpty(meta.getSnapshot())) {
                    Map<String, Object> checkpoint = AgentOffsetUtil.readCheckpoint(meta.getSnapshot());
                    minCheckpoint = AgentOffsetUtil.min(minCheckpoint, checkpoint);
                }
            }
        }
        map.put("mappingNames", mappingNames);
        map.put("mappingCount", mappingNames.size());
        map.put("mappingNamesText", mappingNames.isEmpty() ? "-" : String.join(", ", mappingNames));
        map.put("subscribeSchemas", new ArrayList<>(schemas));
        map.put("subscribeText", schemas.isEmpty() ? "-" : String.join(", ", schemas));
        map.put("checkpoint", minCheckpoint);
        map.put("checkpointText", formatCheckpoint(minCheckpoint));

        Integer queueCapacity = task.getQueueCapacity() == null || task.getQueueCapacity() <= 0
                ? 100000 : task.getQueueCapacity();
        Integer queueSize = null;
        Map<String, Object> runtime = captureAgentService.getRemoteRuntimeInfo(task.getId());
        if (runtime != null) {
            if (runtime.get("queueSize") != null) {
                queueSize = NumberUtil.toInt(String.valueOf(runtime.get("queueSize")), -1);
                if (queueSize < 0) {
                    queueSize = null;
                }
            }
            if (runtime.get("queueCapacity") != null) {
                int runtimeCapacity = NumberUtil.toInt(String.valueOf(runtime.get("queueCapacity")), -1);
                if (runtimeCapacity > 0) {
                    queueCapacity = runtimeCapacity;
                }
            }
        }
        map.put("queueSize", queueSize);
        map.put("queueCapacity", queueCapacity);
        map.put("queueCapacityText", queueSize == null
                ? String.valueOf(queueCapacity)
                : queueSize + "/" + queueCapacity);
    }

    private static String formatHostPort(String host, Integer port) {
        if (StringUtil.isBlank(host)) {
            return "-";
        }
        if (port == null || port <= 0) {
            return host;
        }
        return host + ":" + port;
    }

    private static String formatCheckpoint(Map<String, Object> checkpoint) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            return "-";
        }
        List<String> parts = new ArrayList<>();
        appendCheckpointPart(parts, "scn", checkpoint.get("scn"));
        appendCheckpointPart(parts, "seq", checkpoint.get("sequence"));
        appendCheckpointPart(parts, "block", checkpoint.get("block"));
        appendCheckpointPart(parts, "offset", checkpoint.get("offset"));
        appendCheckpointPart(parts, "path", checkpoint.get("path"));
        return parts.isEmpty() ? "-" : String.join(", ", parts);
    }

    private static void appendCheckpointPart(List<String> parts, String label, Object value) {
        if (value == null) {
            return;
        }
        String text = String.valueOf(value).trim();
        if (StringUtil.isBlank(text) || "null".equalsIgnoreCase(text)) {
            return;
        }
        parts.add(label + "=" + text);
    }

    private Map<String, Object> baseView(String id, String name, String type, Long createTime, Long updateTime) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", id);
        map.put("name", name);
        map.put("type", type);
        map.put("createTime", createTime);
        map.put("updateTime", updateTime);
        Meta meta = taskMetaProfile.getMeta(id);
        map.put("state", meta == null ? CommonTaskStatusEnum.READY.getCode() : meta.getState());
        return map;
    }
}
