package org.dbsyncer.biz.vo;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 0:14
 */
public class UserVo {

    /**
     * 账号
     */
    private String username;

    /**
     * 昵称
     */
    private String nickname;

    public UserVo(String username) {
        this(username, username);
    }

    public UserVo(String username, String nickname) {
        this.username = username;
        this.nickname = nickname;
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
}