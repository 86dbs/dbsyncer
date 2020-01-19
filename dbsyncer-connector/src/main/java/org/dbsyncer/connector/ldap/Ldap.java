package org.dbsyncer.connector.ldap;

import org.dbsyncer.connector.Connector;
import org.dbsyncer.connector.config.LdapConfig;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.CommunicationException;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.LdapTemplate;

public interface Ldap extends Connector {

    LdapTemplate getLdapTemplate(LdapConfig config) throws AuthenticationException, CommunicationException, javax.naming.NamingException;

    void close(DirContextAdapter ctx);

}
