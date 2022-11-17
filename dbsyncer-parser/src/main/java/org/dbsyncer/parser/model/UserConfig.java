package org.dbsyncer.parser.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 用户配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/11/17 22:54
 */
public class UserConfig extends ConfigModel {

    private List<UserInfo> userInfoList = new ArrayList<>();

    public synchronized void addUserInfo(UserInfo userInfo){
        if(userInfoList.contains(userInfo)){
            return;
        }
        userInfoList.add(userInfo);
    }

    public synchronized void updateUserInfo(UserInfo userInfo){
        for (UserInfo u : userInfoList) {
            if(u.getUsername().equals(userInfo.getUsername())){
                u.setNickname(userInfo.getNickname());
                u.setPassword(userInfo.getPassword());
                break;
            }
        }
    }

    public synchronized void removeUserInfo(String username){
        Iterator<UserInfo> iterator = userInfoList.iterator();
        while (iterator.hasNext()){
            UserInfo next = iterator.next();
            if(next.getUsername().equals(username)){
                iterator.remove();
                break;
            }
        }
    }

    public synchronized UserInfo getUserInfo(String username){
        Iterator<UserInfo> iterator = userInfoList.iterator();
        while (iterator.hasNext()){
            UserInfo next = iterator.next();
            if(next.getUsername().equals(username)){
                return next;
            }
        }
        return null;
    }

    public List<UserInfo> getUserInfoList() {
        return userInfoList;
    }

    public void setUserInfoList(List<UserInfo> userInfoList) {
        this.userInfoList = userInfoList;
    }
}
