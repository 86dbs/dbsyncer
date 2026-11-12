/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.AgentConfigService;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.RSAUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.AgentProfile;
import org.dbsyncer.parser.model.AgentConfig;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link AgentConfigService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
@Service
public class AgentConfigServiceImpl implements AgentConfigService {

    private static final int STATE_RUNNING = 1;

    @Resource
    private AgentProfile agentProfile;

    @Override
    public List<Map<String, Object>> list() {
        List<AgentConfig> configs = agentProfile.listAll();
        List<Map<String, Object>> result = new ArrayList<>(configs.size());
        for (AgentConfig config : configs) {
            result.add(toView(config));
        }
        return result;
    }

    @Override
    public Map<String, Object> get(String id) {
        AgentConfig config = agentProfile.get(id);
        return config == null ? null : toView(config);
    }

    @Override
    public String add(Map<String, String> params) {
        AgentConfig config = build(params, null);
        RsaVersion keys = RSAUtil.createKeys(2048);
        config.setPublicKey(keys.getPublicKey());
        config.setPrivateKey(keys.getPrivateKey());
        config.setState(0);
        agentProfile.add(config);
        return config.getId();
    }

    @Override
    public String edit(Map<String, String> params) {
        String id = params.get("id");
        Assert.hasText(id, "ID不能为空");
        AgentConfig existing = agentProfile.get(id);
        Assert.notNull(existing, "Agent不存在");
        if (existing.getState() != null && existing.getState() == STATE_RUNNING) {
            throw new BizException("运行中的 Agent 不能编辑，请先停止");
        }
        AgentConfig config = build(params, existing);
        agentProfile.edit(config);
        return config.getId();
    }

    @Override
    public void remove(String id) {
        Assert.hasText(id, "ID不能为空");
        AgentConfig existing = agentProfile.get(id);
        Assert.notNull(existing, "Agent不存在");
        if (existing.getState() != null && existing.getState() == STATE_RUNNING) {
            throw new BizException("运行中的 Agent 不能删除，请先停止");
        }
        agentProfile.remove(id);
    }

    @Override
    public String generateSecret() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private AgentConfig build(Map<String, String> params, AgentConfig existing) {
        String name = StringUtil.trim(params.get("name"));
        Assert.hasText(name, "名称不能为空");
        int listenPort = NumberUtil.toInt(params.get("listenPort"), 0);
        Assert.isTrue(listenPort > 0 && listenPort <= 65535, "监听端口不合法");
        String agentSecret = StringUtil.trim(params.get("agentSecret"));
        Assert.hasText(agentSecret, "AGENT密钥不能为空");
        String jdbcUrl = StringUtil.trim(params.get("jdbcUrl"));
        String username = StringUtil.trim(params.get("username"));
        String schema = StringUtil.trim(params.get("schema"));
        Assert.hasText(jdbcUrl, "JDBC URL不能为空");
        Assert.hasText(username, "用户名不能为空");

        String password = params.get("password");
        if (existing != null && StringUtil.isBlank(password)) {
            Map old = JsonUtil.parseMap(existing.getJson());
            if (old != null && old.get("password") != null) {
                password = String.valueOf(old.get("password"));
            }
        }
        Assert.hasText(password, "密码不能为空");

        Map<String, Object> source = new HashMap<>();
        source.put("jdbcUrl", jdbcUrl);
        source.put("username", username);
        source.put("password", password);
        source.put("schema", schema);

        AgentConfig config = existing == null ? new AgentConfig() : existing;
        config.setName(name);
        config.setListenPort(listenPort);
        config.setAgentSecret(agentSecret);
        config.setThreadNum(NumberUtil.toInt(params.get("threadNum"), 4));
        config.setCaptureIntervalMs(NumberUtil.toInt(params.get("captureIntervalMs"), 1000));
        String checkpoint = StringUtil.trim(params.get("checkpoint"));
        config.setCheckpoint(StringUtil.isBlank(checkpoint) ? "tail.ckpt" : checkpoint);
        config.setJson(JsonUtil.objToJson(source));
        return config;
    }

    private Map<String, Object> toView(AgentConfig config) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", config.getId());
        map.put("name", config.getName());
        map.put("listenPort", config.getListenPort());
        map.put("agentSecret", config.getAgentSecret());
        map.put("threadNum", config.getThreadNum());
        map.put("captureIntervalMs", config.getCaptureIntervalMs());
        map.put("checkpoint", config.getCheckpoint());
        map.put("publicKey", config.getPublicKey());
        map.put("json", config.getJson());
        map.put("state", config.getState());
        map.put("createTime", config.getCreateTime());
        map.put("updateTime", config.getUpdateTime());
        return map;
    }
}
