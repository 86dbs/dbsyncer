package org.dbsyncer.parser;

import org.dbsyncer.common.model.Result;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.Task;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.model.MetaInfo;
import org.dbsyncer.sdk.plugin.PluginContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * <pre>
 *     实现全量和增量同步任务，以及连接器读写，插件转换，表元信息，心跳检测
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/20 21:11
 */
public interface ParserComponent {

    /**
     * 获取表元信息
     *
     * @param context
     * @return
     */
    List<MetaInfo> getMetaInfo(DefaultConnectorServiceContext context);

    /**
     * 获取映射关系执行命令
     *
     * @param mapping
     * @param tableGroup
     * @return
     */
    Map<String, String> getCommand(Mapping mapping, TableGroup tableGroup);

    /**
     * 全量同步
     *
     * @param task
     * @param mapping
     * @param tableGroup
     * @param executor
     */
    void execute(Task task, Mapping mapping, TableGroup tableGroup, Executor executor);

    /**
     * 批执行
     *
     * @param pluginContext
     * @param executor
     * @return
     */
    Result writeBatch(PluginContext pluginContext, Executor executor);

}