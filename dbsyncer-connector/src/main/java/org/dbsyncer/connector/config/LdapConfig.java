package org.dbsyncer.connector.config;

import java.util.List;

public class LdapConfig extends ConnectorConfig {

    // 连接地址 ldap://127.0.0.1:389/
    private String url;

    // 访问域 ou=scdl,dc=hello,dc=com
    private String base;

    // 帐号 cn=Directory Manager
    private String userDn;

    // 密码 secret
    private String password;

    // 域 dn
    private String dn;

    // 继承对象
    private List<String> extendObjects;

    // 查找范围,支持同级或递归
    private String searchScope;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public String getUserDn() {
        return userDn;
    }

    public void setUserDn(String userDn) {
        this.userDn = userDn;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public List<String> getExtendObjects() {
        return extendObjects;
    }

    public void setExtendObjects(List<String> extendObjects) {
        this.extendObjects = extendObjects;
    }

    public String getSearchScope() {
        return searchScope;
    }

    public void setSearchScope(String searchScope) {
        this.searchScope = searchScope;
    }

}