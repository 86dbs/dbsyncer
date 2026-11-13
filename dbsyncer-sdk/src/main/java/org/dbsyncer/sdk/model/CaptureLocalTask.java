/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 本地采集任务（{@code TYPE=captureLocal}），配置持久化到 {@code dbsyncer_task.JSON}。
 * <p>本机作为 Netty 服务端，采集源库 CDC 并推送。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public class CaptureLocalTask extends ConfigModel {

    public CaptureLocalTask() {
        super.setType(ConfigConstant.CAPTURE_LOCAL);
    }

    /**
     * Netty 监听端口
     */
    private Integer listenPort;

    /**
     * 会话认证密钥
     */
    private String agentSecret;

    /**
     * 采集/解析线程数
     */
    private Integer threadNum = 4;

    /**
     * 采集频率（毫秒）
     */
    private Integer captureIntervalMs = 1000;

    /**
     * 位点文件名（内容在文件中）
     */
    private String checkpoint = "tail.ckpt";

    private String publicKey;
    private String privateKey;

    /**
     * 源库 JDBC URL
     */
    private String jdbcUrl;

    private String username;
    private String password;

    /**
     * Schema，可选
     */
    private String schema;

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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
