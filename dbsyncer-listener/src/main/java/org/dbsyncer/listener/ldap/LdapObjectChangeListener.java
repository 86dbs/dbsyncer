package org.dbsyncer.listener.ldap;

import org.dbsyncer.connector.constant.ConnectorConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.event.NamingEvent;
import javax.naming.event.NamingExceptionEvent;
import javax.naming.event.ObjectChangeListener;

public class LdapObjectChangeListener implements ObjectChangeListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private LdapListener ldapListener;

    public LdapObjectChangeListener(LdapListener ldapListener) {
        this.ldapListener = ldapListener;
    }

    public void setLdapListener(LdapListener ldapListener) {
        this.ldapListener = ldapListener;
    }

    // 更新
    @Override
    public void objectChanged(NamingEvent evt) {
        ldapListener.parseLog(ConnectorConstant.OPERTION_UPDATE, evt);
    }

    // 异常监听
    @Override
    public void namingExceptionThrown(NamingExceptionEvent evt) {
        logger.error("异常监听", evt.getException());
    }

}
