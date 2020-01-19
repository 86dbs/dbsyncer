package org.dbsyncer.listener.ldap;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.LdapConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.constant.LdapConstant;
import org.dbsyncer.listener.AbstractListener;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.*;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.event.EventContext;
import javax.naming.event.NamespaceChangeListener;
import javax.naming.event.NamingEvent;
import javax.naming.event.ObjectChangeListener;
import java.util.Hashtable;

/**
 * LdapListener 事件监听器
 *
 * @author AE86
 * @ClassName: LdapListener
 * @Description: 监听ldap新增、修改、刪除操作
 * @date: 2017年8月22日 下午6:39:05
 */
public final class LdapListener extends AbstractListener {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private EventContext context;

    // 新增、删除、重命名属性事件监听
    private NamespaceChangeListener namespaceChangeListener;

    // 修改事件监听
    private ObjectChangeListener objectChangeListener;

    @Override
    public void onClose() {
        synchronized (context) {
            try {
                // 移除监听器
                context.removeNamingListener(objectChangeListener);
                context.removeNamingListener(namespaceChangeListener);
                // 关闭上下文
                context.close();
                logger.info("LdapListener has been closed.");
            } catch (NamingException e) {
                logger.error("Close the LdapListener error", e.getLocalizedMessage());
            }
        }
    }

    @Override
    public void run() {
//        // 启动监听线程
//        // 1、获取驱动
//        MappingTask mapping = null;
//
//        // 2、检查驱动连接器类型
//        Mapping sMapping = mapping.getSourceMapping();
//        String connector = sMapping.getConnector();
//        if (!StringUtils.equals(ConnectorEnum.LDAP.getName(), connector)) {
//            return;
//        }

        // 3、创建监听上下文
//        LdapConfig c = (LdapConfig) sMapping.getConfig();
        LdapConfig c = null;
        try {
            context = getContext(c);
            if (null != context) {
                // 4、注册监听器
                String scope = c.getSearchScope(); // 域 dn
                Integer subtreeScope = LdapConstant.OPERS.get(scope);

                // 绑定新增、删除、重命名、修改事件监听
                namespaceChangeListener = new LdapNamespaceChangeListener(this);
                objectChangeListener = new LdapObjectChangeListener(this);
                context.addNamingListener("", subtreeScope, namespaceChangeListener);
                context.addNamingListener("", subtreeScope, objectChangeListener);
                logger.info("LdapListener has been started.");
            }
        } catch (NamingException e) {
            logger.error("Can not create LdapListener for driver ID ", e.getLocalizedMessage());
        }
    }

    private EventContext getContext(LdapConfig c) throws NamingException {
        String url = c.getUrl(); // 连接地址 ldap://127.0.0.1:389/
        String baseDN = c.getBase(); // 访问域 ou=scdl,dc=hello,dc=com
        String principal = c.getUserDn(); // 帐号 cn=Directory Manager
        String credentials = c.getPassword(); // 密码 secret

        Hashtable<String, Object> env = new Hashtable<String, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        env.put(Context.SECURITY_PRINCIPAL, principal);
        env.put(Context.SECURITY_CREDENTIALS, credentials);
        InitialContext initialContext = new InitialContext(env);
        return (EventContext) initialContext.lookup(baseDN);
    }

    /**
     * 检查消息类型是否为SearchResult，否则抛出异常
     *
     * @param binding 消息
     * @throws IllegalArgumentException
     * @Title: validateBinding
     * @Description: 检查消息类型是否为SearchResult，否则抛出异常
     * @return: SearchResult
     */
    private SearchResult validateBinding(Binding binding) throws IllegalArgumentException {
        if (!(binding instanceof SearchResult)) {
            throw new IllegalArgumentException("Parameter must be an instance of SearchResult");
        }
        return (SearchResult) binding;
    }

    /**
     * 解析增量属性
     *
     * @param searchResult 增量属性
     * @throws IllegalArgumentException
     * @throws NamingException
     * @throws JSONException
     * @Title: parseSearchResult
     * @Description: 解析增量属性
     * @return: void
     */
    private JSONArray parseSearchResult(SearchResult searchResult) throws IllegalArgumentException, NamingException, JSONException {
        // 属性
        Attributes attributes = searchResult.getAttributes();
        if (attributes == null) {
            throw new IllegalArgumentException("The properties of the sync are empty!");
        }

        JSONArray row = new JSONArray();
        // 获取属性名
        NamingEnumeration<String> iDs = attributes.getIDs();
        while (iDs.hasMoreElements()) {
            String attrName = (String) iDs.nextElement();
            // 在LDAP中，同属性名可能有多个，所以获取的时候应该用集合
            Attribute atts = attributes.get(attrName);
            int size = atts.size();
            // 排除无效的属性
            if (size < 1) {
                continue;
            }
            // 遍历属性
            JSONArray list = new JSONArray();
            for (int i = 0; i < size; i++) {
                list.put(atts.get(i));
            }
            // 组装属性名和值
            JSONObject attr = new JSONObject();
            attr.put("name", attrName);
            attr.put("value", list);
            row.put(attr);
        }
        return row;
    }

    /**
     * 解析增量事件
     *
     * @param opr 事件类型
     * @param evt 监听的事件包装（包含监听的服务器配置、变化前后的数据）
     * @Title: parseLog
     * @Description: 解析增量事件，将解析后的日志发送到消息队列中
     * @return: void
     */
    public void parseLog(String opr, NamingEvent evt) {
        // 1、排除非新增、修改和删除操作
        if (!StringUtils.equals(ConnectorConstant.OPERTION_INSERT, opr)
                && !StringUtils.equals(ConnectorConstant.OPERTION_UPDATE, opr)
                && !StringUtils.equals(ConnectorConstant.OPERTION_DELETE, opr)) {
            return;
        }

        JSONArray msg = new JSONArray();
        try {
            // 2、封装增量消息格式
            JSONObject row = new JSONObject();
            Binding binding = evt.getNewBinding();
            row.put("base", binding.getName());
            row.put("eventType", opr);
            SearchResult sr = validateBinding(binding);
            switch (opr) {
                case ConnectorConstant.OPERTION_INSERT:
                    row.put("before", new JSONArray());
                    row.put("after", parseSearchResult(sr));
                    break;
                case ConnectorConstant.OPERTION_UPDATE:
                    row.put("before", new JSONArray());
                    row.put("after", parseSearchResult(sr));
                    break;
                case ConnectorConstant.OPERTION_DELETE:
                    row.put("before", parseSearchResult(sr));
                    row.put("after", new JSONArray());
                    break;
                default:
                    break;
            }
            msg.put(row);
        } catch (NamingException e) {
            logger.error(e.getMessage());
        } catch (JSONException e) {
            logger.error(e.getMessage());
        }

        // sendMsg(msg);
    }

}