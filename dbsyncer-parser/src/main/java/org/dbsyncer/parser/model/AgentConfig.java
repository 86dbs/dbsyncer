/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

/**
 * CDC Agent 配置（对应 {@code dbsyncer_agent}）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public class AgentConfig {

    private String id;
    private String name;
    private Integer listenPort;
    private String agentSecret;
    private Integer threadNum;
    private Integer captureIntervalMs;
    private String checkpoint;
    private String publicKey;
    private String privateKey;
    private String json;
    private Integer state;
    private Long createTime;
    private Long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getListenPort() {
        return listenPort;
    }

    public void setListenPort(Integer listenPort) {
        this.listenPort = listenPort;
    }

    public String getAgentSecret() {
        return agentSecret;
    }

    public void setAgentSecret(String agentSecret) {
        this.agentSecret = agentSecret;
    }

    public Integer getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(Integer threadNum) {
        this.threadNum = threadNum;
    }

    public Integer getCaptureIntervalMs() {
        return captureIntervalMs;
    }

    public void setCaptureIntervalMs(Integer captureIntervalMs) {
        this.captureIntervalMs = captureIntervalMs;
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
}
