package org.dbsyncer.listener.ldap;

import org.dbsyncer.connector.constant.ConnectorConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.event.NamespaceChangeListener;
import javax.naming.event.NamingEvent;
import javax.naming.event.NamingExceptionEvent;

public class LdapNamespaceChangeListener implements NamespaceChangeListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private LdapListener ldapListener;

    public LdapNamespaceChangeListener(LdapListener ldapListener) {
        this.ldapListener = ldapListener;
    }

    public void setLdapListener(LdapListener ldapListener) {
        this.ldapListener = ldapListener;
    }

    // 新增
    @Override
    public void objectAdded(NamingEvent evt) {
        ldapListener.parseLog(ConnectorConstant.OPERTION_INSERT, evt);
    }

    // 删除
    @Override
    public void objectRemoved(NamingEvent evt) {
        ldapListener.parseLog(ConnectorConstant.OPERTION_DELETE, evt);
    }

    // 重命名域名称 template.rename("uid=zhangsan", "uid=zhangsan666");
    @Override
    public void objectRenamed(NamingEvent evt) {
    }

    // 异常监听
    @Override
    public void namingExceptionThrown(NamingExceptionEvent evt) {
        logger.error("异常监听", evt.getException());
    }

}
