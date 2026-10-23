/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 告警 @ 覆盖规则：按分组或同步任务指定手机号（多人逗号分隔）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class NoticeMentionRule extends ConfigModel {

    /**
     * 作用域：{@code group} / {@code mapping}
     */
    private String scopeType;

    /**
     * 分组 ID 或同步任务 ID
     */
    private String scopeId;

    /**
     * 是否 @所有人：0 否 / 1 是
     */
    private int atAll;

    /**
     * @用户手机号，多个用逗号分隔
     */
    private String atMobiles;

    public NoticeMentionRule() {
        super.setName("");
        super.setType(ConfigConstant.NOTICE_MENTION_RULE);
    }

    public String getScopeType() {
        return scopeType;
    }

    public void setScopeType(String scopeType) {
        this.scopeType = scopeType;
    }

    public String getScopeId() {
        return scopeId;
    }

    public void setScopeId(String scopeId) {
        this.scopeId = scopeId;
    }

    public int getAtAll() {
        return atAll;
    }

    public void setAtAll(int atAll) {
        this.atAll = atAll;
    }

    public boolean isAtAll() {
        return atAll == 1;
    }

    public String getAtMobiles() {
        return atMobiles;
    }

    public void setAtMobiles(String atMobiles) {
        this.atMobiles = atMobiles;
    }
}
