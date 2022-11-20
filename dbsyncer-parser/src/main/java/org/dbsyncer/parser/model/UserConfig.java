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

    public void removeUserInfo(String username){
        Iterator<UserInfo> iterator = userInfoList.iterator();
        while (iterator.hasNext()){
            UserInfo next = iterator.next();
            if(next.getUsername().equals(username)){
                iterator.remove();
                break;
            }
        }
    }

    public UserInfo getUserInfo(String username){
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
