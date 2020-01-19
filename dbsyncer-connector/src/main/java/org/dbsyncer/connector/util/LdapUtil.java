package org.dbsyncer.connector.util;

import org.dbsyncer.connector.config.Field;
import org.dbsyncer.connector.config.LdapConfig;
import org.dbsyncer.connector.config.MetaInfo;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.CommunicationException;
import org.springframework.ldap.NamingException;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.ldap.support.LdapUtils;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public abstract class LdapUtil {

    private LdapUtil() {
    }

    /**
     * 如果连接失败,会抛出异常
     *
     * @param config
     * @return LdapTemplate
     * @throws AuthenticationException
     * @throws CommunicationException
     * @throws javax.naming.NamingException
     */
    public static LdapTemplate getLdapTemplate(LdapConfig config) throws AuthenticationException, CommunicationException, javax.naming.NamingException {
        // 配置Ldap基本配置
        LdapContextSource ctx = new LdapContextSource();
        ctx.setUrl(config.getUrl());
        ctx.setBase(config.getBase());
        ctx.setUserDn(config.getUserDn());
        ctx.setPassword(config.getPassword());
        // 验证所有必要的参数
        ctx.afterPropertiesSet();
        LdapTemplate t = new LdapTemplate(ctx);
        // If the account password is incorrect, an exception will be thrown.
        DirContext context = t.getContextSource().getContext(config.getUserDn(), config.getPassword());
        if (null != context) {
            context.close();
        }
        return t;
    }

    public static void closeContext(DirContextAdapter ctx) {
        LdapUtils.closeContext(ctx);
    }

    /**
     * 获取连接器基本信息
     *
     * @param ldapTemplate
     * @param filter
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static MetaInfo getMetaInfo(LdapTemplate ldapTemplate, String filter) {
        // 转换器
        AttributesMapper mapper = new AttributesMapper() {
            public Object mapFromAttributes(Attributes attrs) throws NamingException, javax.naming.NamingException {
                return attrs;
            }
        };

        // 只获取第一条数据
        ldapTemplate.setDefaultCountLimit(1);
        List<Attributes> search = ldapTemplate.search("", filter, mapper);

        // 获取属性名
        if (search == null || search.isEmpty()) {
            return new MetaInfo(null, 0);
        }
        Attributes attributes = search.get(0);
        // 获取属性名
        NamingEnumeration<String> iDs = attributes.getIDs();

        // 组装元数据
        List<Field> fields = new ArrayList<>();
        while (iDs.hasMoreElements()) {
            String name = (String) iDs.nextElement();
            if (name.equalsIgnoreCase("objectClass")) {
                continue;
            }
            // 默认都是字符类型
            fields.add(new Field(name, "VARCHAR", Types.VARCHAR));
        }

        // 关闭连接
        if (null != iDs) {
            try {
                iDs.close();
            } catch (javax.naming.NamingException e) {
            }
        }

        // 获取数据总量
        return new MetaInfo(fields, 0);
    }

}
