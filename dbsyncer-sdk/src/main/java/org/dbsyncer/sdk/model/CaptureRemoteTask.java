/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 远端采集任务（{@code TYPE=captureRemote}），配置持久化到 {@code dbsyncer_task.JSON}。
 * <p>本机作为 Netty 客户端，连接本地采集任务收事件。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public class CaptureRemoteTask extends ConfigModel {

    public CaptureRemoteTask() {
        super.setType(ConfigConstant.CAPTURE_REMOTE);
    }

    private String agentHost;
    private Integer agentPort;
    private String agentSecret;
    private Integer queueCapacity = 100000;
    private String publicKey;
    private String privateKey;

    public String getAgentHost() {
        return agentHost;
    }

    public void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public Integer getAgentPort() {
        return agentPort;
    }

    public void setAgentPort(Integer agentPort) {
        this.agentPort = agentPort;
    }

    public String getAgentSecret() {
        return agentSecret;
    }

    public void setAgentSecret(String agentSecret) {
        this.agentSecret = agentSecret;
    }

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(Integer queueCapacity) {
        this.queueCapacity = queueCapacity;
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
}
