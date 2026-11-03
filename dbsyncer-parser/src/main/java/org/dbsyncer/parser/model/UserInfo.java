package org.dbsyncer.parser.model;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 23:09
 */
public class UserInfo {

    /**
     * 唯一ID
     */
    private String id;

    /**
     * 创建时间
     */
    private Long createTime;

    /**
     * 更新时间
     */
    private Long updateTime;

    /**
     * 账号
     */
    private String username;

    /**
     * 昵称
     */
    private String nickname;

    /**
     * 密码
     */
    private String password;

    /**
     * 角色
     */
    private String roleCode;

    /**
     * 连续登录失败次数
     */
    private int loginFailCount;

    /**
     * 锁定截止时间（毫秒时间戳，0 表示未锁定）
     */
    private long lockUntil;

    public UserInfo() {
    }

    public UserInfo(String username, String nickname, String password, String roleCode) {
        this.username = username;
        this.nickname = nickname;
        this.password = password;
        this.roleCode = roleCode;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRoleCode() {
        return roleCode;
    }

    public void setRoleCode(String roleCode) {
        this.roleCode = roleCode;
    }

    public int getLoginFailCount() {
        return loginFailCount;
    }

    public void setLoginFailCount(int loginFailCount) {
        this.loginFailCount = loginFailCount;
    }

    public long getLockUntil() {
        return lockUntil;
    }

    public void setLockUntil(long lockUntil) {
        this.lockUntil = lockUntil;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UserInfo) {
            UserInfo userInfo = (UserInfo) obj;
            return userInfo.username.equals(this.username);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }
}
