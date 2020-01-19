package org.dbsyncer.connector.constant;

import javax.naming.directory.SearchControls;
import java.util.HashMap;
import java.util.Map;

public class LdapConstant {

    /**
     * 同级查询
     */
    public static final String LDAP_ONELEVEL_SCOPE = "onelevel";

    /**
     * 递归查询
     */
    public static final String LDAP_SUBTREE_SCOPE = "subtree";

    /**
     * 查询方式
     */
    public static final Map<String, Integer> OPERS = new HashMap<String, Integer>();

    static {
        OPERS.put(LDAP_ONELEVEL_SCOPE, SearchControls.ONELEVEL_SCOPE);
        OPERS.put(LDAP_SUBTREE_SCOPE, SearchControls.SUBTREE_SCOPE);
    }

}
