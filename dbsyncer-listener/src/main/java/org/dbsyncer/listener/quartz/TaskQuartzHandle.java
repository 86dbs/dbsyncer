package org.dbsyncer.listener.quartz;/*
package org.dbsyncer.connector.quartz;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.common.constant.ConnectorConstant;
import org.dbsyncer.common.constant.MappingConstant;
import org.dbsyncer.common.entity.DatabaseConfig;
import org.dbsyncer.common.entity.Mapping;
import org.dbsyncer.common.entity.MappingTask;
import org.dbsyncer.connector.ConnectorFactory;
import org.dbsyncer.connector.database.Database;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskQuartzHandle {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // 正在运行的job
    private static Map<String, Boolean> running = new ConcurrentHashMap<String, Boolean>();

    */
/**
 * <ul>
 * <dt>即将面临的问题如下：</dt>
 * <dl>
 * <li>1.如果上一次任务未处理完成,而当前又产生了新的任务,此时多个任务可能会在同一时间执行相同工作,会造成重复数据.</li>
 * <li>2.如果上一次任务未处理完成,而当前又产生了新的任务,长时间下去,可能会造成任务堆积,影响系统性能.</li>
 * </dl>
 * </ul>
 * <ul>
 * <dt>相应的解决方法：</dt>
 * <dl>
 * <li>1.创建集合,存放执行的任务</li>
 * <li>2.从集合里检查上一次任务存在,继续执行.</li>
 * <li>3.从集合里检查上一次任务不存在,拒绝当前任务.</li>
 * </dl>
 * </ul>
 *
 * @param taskId
 * <p>
 * 组装数据格式
 * @param msg
 * @param eventType
 * @param beforeArr
 * @param afterArr
 * @throws JSONException
 * <p>
 * 解析操作事件,用","分割成map集合
 * @param event
 * @param filter
 * <p>
 * 根据增量策略过滤数据
 * @param inc  增量策略
 * @param rows 数据
 * @return 过滤数据
 * <p>
 * 刷新最后记录点
 * @param task
 * @param inc
 * @param scnPos
 * <p>
 * 组装数据格式
 * @param msg
 * @param eventType
 * @param beforeArr
 * @param afterArr
 * @throws JSONException
 * <p>
 * 解析操作事件,用","分割成map集合
 * @param event
 * @param filter
 * <p>
 * 根据增量策略过滤数据
 * @param inc  增量策略
 * @param rows 数据
 * @return 过滤数据
 * <p>
 * 刷新最后记录点
 * @param task
 * @param inc
 * @param scnPos
 *//*

    public void handle(String taskId) {
        // 1.检查是否存在
        Boolean flg = running.get(taskId);
        if (null != flg && flg) {
            logger.error("the task ID \"" + taskId + "\" is running.");
            return;
        }
        // 2.添加驱动job
        running.put(taskId, true);
        try {
            // 3.获取驱动配置
//            MappingTask task = data.getMapping(taskId);
//            // 4.校验是否驱动配置为空
//            if (null == task) {
//                running.remove(taskId);
//                logger.error("the task ID \"" + taskId + "\" can not be null.");
//                return;
//            }
//            // 5.提交任务
//            this.submit(task);
        } catch (Exception e) {
            logger.error(e.getClass() + " >> " + e.getLocalizedMessage());
        } finally {
            // 6.执行结束,移除正在执行的job
            running.remove(taskId);
        }
    }

    private void submit(MappingTask task) {
        Mapping mapping = task.getSourceMapping();
        Database connector = ConnectorFactory.getInstance().getConnector(mapping.getConnector(), Database.class);

        // 1.建立连接
        JdbcTemplate jdbcTemplate = null;
        try {
            DatabaseConfig config = (DatabaseConfig) task.getSourceMapping().getConfig();
            jdbcTemplate = connector.getJdbcTemplate(config);
            // 2.解析最新的数据
            JSONArray msg = this.pull(jdbcTemplate, task);

            // 3.将增量消息发送给manager处理
            if (null != msg && 0 < msg.length()) {
                JSONObject t = new JSONObject();
                t.put("taskId", task.getId());
                t.put("msg", msg);
//                manager.handle(t);
            }
        } catch (Exception e) {
            logger.error(e.getClass() + " >> " + e.getLocalizedMessage());
        } finally {
            if (null != connector) {
                // 断开连接
                connector.close(jdbcTemplate);
            }
        }
    }

    private JSONArray pull(JdbcTemplate jdbcTemplate, MappingTask task) {
        // 1. 获取执行命令
        Map<String, String> executeCommond = task.getSourceMapping().getExecuteCommond();

        // 2. 获取最近记录点
        // 获取最近时间 SELECT MAX(LASTDATE) FROM ASD_TEST;
        String queryMax = executeCommond.get(ConnectorConstant.OPERTION_QUERY_QUARTZ_MAX);
        Timestamp scnPos = this.getScnPos(jdbcTemplate, queryMax);
        // 没有扫描到数据直接返回
        if (null == scnPos) {
            return null;
        }

        // 3.读取最近记录点
        Map<String, Map<String, String>> po = task.getPolicy();
        Map<String, String> inc = po.get(MappingConstant.POLICY_INCREMENT);
        String lastScnPosStr = inc.get("scnPos");
        String sql = executeCommond.get(ConnectorConstant.OPERTION_QUERY_QUARTZ);
        Object[] args = null;
        // 4.没有记录,证明是首次读取数据,查询>=lastScnPosStr的所有数据
        if (StringUtils.isBlank(lastScnPosStr) || StringUtils.equals("null", StringUtils.trim(lastScnPosStr))) {
            sql += executeCommond.get(ConnectorConstant.OPERTION_QUERY_QUARTZ_ALL);
            args = new Object[]{scnPos};
        } else {
            // 5. 防止时间戳格式不正确
            Timestamp lastScnPos = null;
            try {
                lastScnPos = Timestamp.valueOf(lastScnPosStr);
            } catch (Exception e1) {
                // 如果时间戳格式不正确,则设置为空,下一次读取重新读取最新的记录点
                this.refreshScnPos(task, inc, null);
                return null;
            }

            // 6. 如果最近记录时间<=上次记录点,证明没有新增数据,直接结束.
            if (null == lastScnPos || scnPos.getTime() <= lastScnPos.getTime()) {
                return null;
            }
            sql += executeCommond.get(ConnectorConstant.OPERTION_QUERY_QUARTZ_RANGE);
            args = new Object[]{lastScnPos, scnPos};
        }

        // 7.刷新最后记录点
        this.refreshScnPos(task, inc, scnPos.toString());

        // 8.获取增量语句  SELECT ASD_TEST.* FROM ASD_TEST LASTDATE > ? AND LASTDATE <= ?
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, args);
        if (null == rows || rows.isEmpty()) {
            return null;
        }

        // 9. 解析增量数据格式
        return this.filterRowsByPolicyIncrement(inc, rows);
    }

    */
/**
 * 组装数据格式
 *
 * @param msg
 * @param eventType
 * @param beforeArr
 * @param afterArr
 * @throws JSONException
 *//*

    private void putRow(JSONArray msg, String eventType, JSONArray beforeArr, JSONArray afterArr) throws JSONException {
        JSONObject row = new JSONObject();
        row.put("eventType", eventType);
        row.put("before", beforeArr);
        row.put("after", afterArr);
        msg.put(row);
    }

    */
/**
 * 解析操作事件,用","分割成map集合
 *
 * @param event
 * @param filter
 *//*

    private void splitEvent(String event, Map<String, Boolean> filter) {
        if (StringUtils.isBlank(event)) {
            return;
        }
        String[] arr = event.split(",");
        if (null == arr) {
            return;
        }
        int len = arr.length;
        String e;
        for (int i = 0; i < len; i++) {
            e = arr[i];
            filter.put(e, true);
        }
    }

    */
/**
 * 根据增量策略过滤数据
 *
 * @param inc  增量策略
 * @param rows 数据
 * @return 过滤数据
 *//*

    private JSONArray filterRowsByPolicyIncrement(Map<String, String> inc, List<Map<String, Object>> rows) {
        // 用于区分事件的字段名称
        String eventFiled = inc.get("quartzEvent");
        // 事件：新增、修改和删除
        String quartzEventInsert = inc.get("quartzEventInsert");
        String quartzEventUpdate = inc.get("quartzEventUpdate");
        String quartzEventDelete = inc.get("quartzEventDelete");

        // 由于新增、修改和删除过滤事件可能有多个组合
        Map<String, Boolean> iFilter = new HashMap<String, Boolean>();
        Map<String, Boolean> uFilter = new HashMap<String, Boolean>();
        Map<String, Boolean> dFilter = new HashMap<String, Boolean>();
        this.splitEvent(quartzEventInsert, iFilter);
        this.splitEvent(quartzEventUpdate, uFilter);
        this.splitEvent(quartzEventDelete, dFilter);

        JSONArray msg = null;
        JSONArray beforeArr = null;
        JSONArray afterArr = null;
        for (Map<String, Object> col : rows) {
            // 创建返回过滤数据对象
            if (null == msg) {
                msg = new JSONArray();
            }
            try {
                // 1.解析增量数据事件
                String event = String.valueOf(col.get(eventFiled));
                if (null != uFilter.get(event)) {
                    // 1.1修改数据
                    afterArr = this.parseColumn(col);
                    // 组装数据格式
                    this.putRow(msg, ConnectorConstant.OPERTION_UPDATE, new JSONArray(), afterArr);
                } else if (null != iFilter.get(event)) {
                    // 1.2新增数据
                    afterArr = this.parseColumn(col);
                    // 组装数据格式
                    this.putRow(msg, ConnectorConstant.OPERTION_INSERT, new JSONArray(), afterArr);
                } else if (null != dFilter.get(event)) {
                    // 1.3删除数据
                    beforeArr = this.parseColumn(col);
                    // 组装数据格式
                    this.putRow(msg, ConnectorConstant.OPERTION_DELETE, beforeArr, new JSONArray());
                }
            } catch (JSONException e) {
                logger.error(e.getClass() + " >> " + e.getLocalizedMessage());
            }
        }

        iFilter = null;
        uFilter = null;
        dFilter = null;
        return msg;
    }

    */
/**
 * 刷新最后记录点
 *
 * @param task
 * @param inc
 * @param scnPos
 *//*

    private synchronized void refreshScnPos(MappingTask task, Map<String, String> inc, String scnPos) {
        // 刷新最后记录点
        inc.put("scnPos", scnPos);
//        data.saveMapping(task.getId(), task);
    }

    // 转换行数据格式为JSONArray
    private JSONArray parseColumn(Map<String, Object> col) throws JSONException {
        JSONArray row = new JSONArray();
        JSONObject attr = null;
        Object value = null;
        for (Entry<String, Object> obj : col.entrySet()) {
            attr = new JSONObject();
            attr.put("name", obj.getKey());
            // 防止为null
            value = obj.getValue();
            value = null == value ? "" : value;
            attr.put("value", value);
            row.put(attr);
        }
        return row;
    }

    // 获取最近记录点
    private Timestamp getScnPos(JdbcTemplate jdbcTemplate, String queryMax) {
        return jdbcTemplate.queryForObject(queryMax, Timestamp.class);
    }

}
*/
