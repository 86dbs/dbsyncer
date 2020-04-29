package org.dbsyncer.connector.ldap;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.task.Result;
import org.dbsyncer.connector.config.*;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.util.LdapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ldap.AuthenticationException;
import org.springframework.ldap.CommunicationException;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.filter.*;
import org.springframework.ldap.filter.Filter;

import java.util.List;
import java.util.Map;

public final class LdapConnector implements Ldap {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public boolean isAlive(ConnectorConfig config) {
		LdapConfig cfg = (LdapConfig) config;
		try {
			LdapUtil.getLdapTemplate(cfg);
			return true;
		} catch (Exception e) {
			logger.error("Failed to connect:" + cfg.getUrl(), e.getMessage());
		}
		return false;
	}

	@Override
	public List<String> getTable(ConnectorConfig config) {
		return null;
	}

	@Override
	public MetaInfo getMetaInfo(ConnectorConfig config, String tableName) {
		return null;
	}

	@Override
	public Map<String, String> getSourceCommand(CommandConfig commandConfig) {
		return null;
	}

	@Override
	public Map<String, String> getTargetCommand(CommandConfig commandConfig) {
		return null;
	}

	@Override
	public Result reader(ConnectorConfig config, Map<String, String> command, int pageIndex, int pageSize) {
		return null;
	}

	@Override
	public Result writer(ConnectorConfig config, Map<String, String> command, int threadSize, List<Field> fields, List<Map<String, Object>> data) {
		return null;
	}

	@Override
	public LdapTemplate getLdapTemplate(LdapConfig config) throws AuthenticationException, CommunicationException, javax.naming.NamingException {
		return LdapUtil.getLdapTemplate(config);
	}

	@Override
	public void close(DirContextAdapter ctx) {
		LdapUtil.closeContext(ctx);
	}

	/**
	 * @param extendObjects
	 *            继承对象
	 * @param query
	 *            过滤的字段，包括and/or条件
	 * @Title: getFilter
	 * @Description: 获取查询条件
	 * @return: String
	 */
	public String getFilter(List<String> extendObjects, Map<String, List<Map<String, String>>> query) {
		// 解析继承对象
		AndFilter filter = new AndFilter();
		for (String obj : extendObjects) {
			filter.and(new EqualsFilter("objectclass", obj));
		}

		// 解析同步的属性
		OrFilter orFilter = new OrFilter();
		if (query != null && !query.isEmpty()) {
			// 拼接并且
			List<Map<String, String>> addList = query.get(ConnectorConstant.OPERTION_QUERY_AND);
			this.getOperFilter(ConnectorConstant.OPERTION_QUERY_AND, orFilter, addList);

			// 拼接或者
			List<Map<String, String>> orList = query.get(ConnectorConstant.OPERTION_QUERY_OR);
			this.getOperFilter(ConnectorConstant.OPERTION_QUERY_OR, orFilter, orList);
		}

		// 添加过滤属性
		String f = orFilter.encode();
		if (!StringUtils.isBlank(f)) {
			filter.and(orFilter);
		}

		String c = filter.encode();
		return StringUtils.isBlank(c) ? "(objectclass=*)" : c;
	}

	private void getOperFilter(String operator, OrFilter filter, List<Map<String, String>> list) {
		if (list == null || list.isEmpty()) {
			return;
		}
		int size = list.size();
		switch (operator) {
		case ConnectorConstant.OPERTION_QUERY_AND:
			AndFilter filterAdd = new AndFilter();
			for (int i = 0; i < size; i++) {
				Map<String, String> c = list.get(i);
				String name = c.get("name");
				String oper = c.get("operator");
				String value = c.get("value");
				// 获取查询条件
				Filter f = this.getFilterByOpr(oper, name, value);
				filterAdd.and(f);
			}
			filter.or(filterAdd);
			break;
		case ConnectorConstant.OPERTION_QUERY_OR:
			OrFilter filterOr = new OrFilter();
			for (int i = 0; i < size; i++) {
				Map<String, String> c = list.get(i);
				String name = c.get("name");
				String oper = c.get("operator");
				String value = c.get("value");
				// 获取查询条件
				Filter f = this.getFilterByOpr(oper, name, value);
				filterOr.or(f);
			}
			filter.or(filterOr);
			break;
		default:
			break;
		}
	}

	private Filter getFilterByOpr(String oper, String name, String value) {
		Filter f = null;
		switch (oper) {
		case "equal":
			f = new EqualsFilter(name, value);
			break;
		case "notEqual":
			f = new NotFilter(new EqualsFilter(name, value));
			break;
		case "gtAndEqual":
			f = new GreaterThanOrEqualsFilter(name, value);
			break;
		case "ltAndEqual":
			f = new LessThanOrEqualsFilter(name, value);
			break;
		default:
			// 默认相等
			f = new EqualsFilter(name, value);
			break;
		}
		return f;
	}

}