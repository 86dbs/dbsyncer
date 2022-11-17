package org.dbsyncer.parser.model;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-29 20:13
 */
public class Config extends ConfigModel {

    private int refreshInterval = 5;

    // 同步失败发送邮箱,多个以分号分割
    private String email;

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(int refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}