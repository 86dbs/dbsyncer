package org.dbsyncer.parser.model;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 23:09
 */
public class UserInfo {

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
     * 邮箱(多个邮箱使用分号拼接)
     */
    private String mail;

    public UserInfo() {
    }

    public UserInfo(String username, String nickname, String password, String roleCode, String mail) {
        this.username = username;
        this.nickname = nickname;
        this.password = password;
        this.roleCode = roleCode;
        this.mail = mail;
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

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof UserInfo){
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