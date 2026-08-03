/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.ManagerFactory;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.command.impl.PreloadCommand;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.enums.GroupStrategyEnum;
import org.dbsyncer.parser.enums.MetaEnum;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Group;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.plugin.PluginFactory;
import org.dbsyncer.plugin.impl.DingTalkNoticeService;
import org.dbsyncer.plugin.impl.HttpNoticeService;
import org.dbsyncer.plugin.impl.MailNoticeService;
import org.dbsyncer.plugin.impl.WeChatNoticeService;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.NoticeChannelEnum;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.notice.MessageService;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

/**
 * 预加载配置模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
@Component
public final class PreloadTemplate implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 版本信息
     */
    public static final String DBS_VERSION_INFO = "versionInfo";

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private ManagerFactory managerFactory;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private StorageService storageService;

    @Resource
    private MessageService messageService;

    @Resource
    private LogService logService;

    @Resource
    private Executor generalExecutor;

    private boolean preloadCompleted;

    @Resource
    private TaskService<ConfigModel> taskService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        // Load plugins
        pluginFactory.loadPlugins();

        // Load Notification Channels
        loadNotificationChannel();

        // Load connectorInstances
        loadConnectorInstance();

        // 同步驱动：按任务级 Meta 恢复 Mapping
        launchSyncMappings();

        // 订正校验 / 整库迁移
        resumeValidateSyncTasks();
        resumeDatabaseSyncTasks();

        preloadCompleted = true;
    }

    public void loadNotificationChannel() {
        try {
            SystemConfig systemConfig = profileComponent.getSystemConfig();
            if (null == systemConfig) {
                return;
            }
            NoticeConfig noticeConfig = systemConfig.getNoticeConfig();
            if (null == noticeConfig) {
                return;
            }

            // 邮件通知
            if (noticeConfig.getMail().isEnabled()) {
                MailNoticeService service = new MailNoticeService();
                service.setUsername(noticeConfig.getMail().getAccount());
                service.setPassword(noticeConfig.getMail().getCode());
                service.build();
                messageService.registerNotifyService(NoticeChannelEnum.EMAIL, service);
            } else {
                messageService.removeNotifyService(NoticeChannelEnum.EMAIL);
            }

            // 企业微信通知
            if (noticeConfig.getWechat().isEnabled()) {
                WeChatNoticeService service = new WeChatNoticeService();
                messageService.registerNotifyService(NoticeChannelEnum.WE_CHAT, service);
            } else {
                messageService.removeNotifyService(NoticeChannelEnum.WE_CHAT);
            }

            // 钉钉通知
            if (noticeConfig.getDingTalk().isEnabled()) {
                DingTalkNoticeService service = new DingTalkNoticeService();
                messageService.registerNotifyService(NoticeChannelEnum.DING_TALK, service);
            } else {
                messageService.removeNotifyService(NoticeChannelEnum.DING_TALK);
            }

            // HTTP通知
            if (noticeConfig.getHttp().isEnabled()) {
                HttpNoticeService service = new HttpNoticeService();
                messageService.registerNotifyService(NoticeChannelEnum.HTTP, service);
            } else {
                messageService.removeNotifyService(NoticeChannelEnum.HTTP);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 是否完成预加载配置
     *
     * @return
     */
    public boolean isPreloadCompleted() {
        return preloadCompleted;
    }

    public void reload(String json) {
        Map<String, Map> map = JsonUtil.jsonToObj(json, Map.class);
        if (CollectionUtils.isEmpty(map)) {
            return;
        }
        // 版本信息检查
        Map versionInfo = map.get(DBS_VERSION_INFO);
        Assert.isTrue(versionInfo != null, "不支持导入低版本或配置不完整");
        VersionInfo info = JsonUtil.jsonToObj(versionInfo.toString(), VersionInfo.class);
        logger.info("upload config: appName={}, version={}, createTime={}", info.getAppName(), info.getVersion(), DateFormatUtil.timestampToString(new Timestamp(info.getCreateTime())));

        // Load configModels
        Stream.of(CommandEnum.PRELOAD_SYSTEM, CommandEnum.PRELOAD_USER, CommandEnum.PRELOAD_CONNECTOR, CommandEnum.PRELOAD_MAPPING, CommandEnum.PRELOAD_META)
                .forEach(commandEnum->reload(map, commandEnum));

        afterConfigImport();
    }

    /**
     * 配置导入完成后的收尾：重建连接实例，恢复同步驱动与企业任务。
     * ZIP / 旧 JSON 导入共用。
     */
    public void afterConfigImport() {
        loadConnectorInstance();
        launchSyncMappings();
        resumeValidateSyncTasks();
        resumeDatabaseSyncTasks();
    }

    /**
     * 恢复同步驱动(Mapping)。
     * <p>只处理任务级 Meta({@code isTaskDetail=0})；明细级 Meta 属于校验/迁移结果或表级进度，不参与驱动启停。
     * Mapping 已并入 {@code dbsyncer_task}，通过 {@link Meta#getTaskId()} 关联。
     */
    private void launchSyncMappings() {
        List<Meta> metas = metaProfile.getTaskMetaAll();
        if (CollectionUtils.isEmpty(metas)) {
            return;
        }
        metas.forEach(meta -> {
            try {
                if (StringUtil.isBlank(meta.getTaskId())) {
                    return;
                }
                Mapping mapping = profileComponent.getMapping(meta.getTaskId());
                // 校验/迁移也有任务级 Meta，但 TYPE 不是 mapping，跳过
                if (mapping == null || !StringUtil.equals(ConfigConstant.MAPPING, mapping.getType())) {
                    return;
                }
                reConnect(mapping);
                // 恢复驱动状态（自动恢复：CDC 监听启动失败时按配置重试）
                if (MetaEnum.RUNNING.getCode() == meta.getState()) {
                    managerFactory.start(mapping, true);
                } else if (MetaEnum.STOPPING.getCode() == meta.getState()) {
                    managerFactory.changeMetaState(meta.getId(), MetaEnum.READY);
                }
            } catch (Exception e) {
                logger.error("恢复同步驱动失败, metaId={}, taskId={}, err={}", meta.getId(), meta.getTaskId(), e.getMessage(), e);
            }
        });
    }

    public void reConnect(Mapping mapping) {
        reConnect(mapping.getId(), mapping.getSourceConnectorId(), mapping.getSourceDatabase(), mapping.getSourceSchema(),
                mapping.getTargetConnectorId(), mapping.getTargetDatabase(), mapping.getTargetSchema());
    }

    public void reConnect(ValidateSyncTask task) {
        //源作为查询，目标也需要作为查询 生成sql语句
        reConnect(task.getId(), task.getSourceConnectorId(), task.getSourceDatabase(), task.getSourceSchema(),
                task.getTargetConnectorId(), task.getTargetDatabase(), task.getTargetSchema());

    }

    public void reConnect(String uniqueId, String sourceConnectorId, String sourceDatabase, String sourceSchema,
                          String targetConnectorId, String targetDatabase, String targetSchema) {
        String sourceInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, sourceConnectorId, ConnectorInstanceUtil.SOURCE_SUFFIX);
        String targetInstanceId = ConnectorInstanceUtil.buildConnectorInstanceId(uniqueId, targetConnectorId, ConnectorInstanceUtil.TARGET_SUFFIX);
        Connector connector = profileComponent.getConnector(sourceConnectorId);
        ConnectorInstance instance = connectorFactory.connect(sourceInstanceId, connector.getConfig(), sourceDatabase, sourceSchema);
        Assert.notNull(instance, "Source connector instance can not null");
        connector = profileComponent.getConnector(targetConnectorId);
        instance = connectorFactory.connect(targetInstanceId, connector.getConfig(), targetDatabase, targetSchema);
        Assert.notNull(instance, "Target connector instance can not null");
    }

    private void reload(Map<String, Map> map, CommandEnum commandEnum) {
        reload(map, commandEnum, commandEnum.getModelType());
    }

    private void reload(Map<String, Map> map, CommandEnum commandEnum, String groupId) {
        Map config = map.get(groupId);
        if (null == config) {
            return;
        }
        Group group = JsonUtil.jsonToObj(config.toString(), Group.class);
        if (null == group || group.isEmpty()) {
            return;
        }

        for (String id : group.getIndex()) {
            Map m = map.get(id);
            ConfigModel model = (ConfigModel) commandEnum.getCommandExecutor().execute(new PreloadCommand(profileComponent, m.toString()));
            operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD, commandEnum.getGroupStrategyEnum()));
            // Load tableGroups
            if (CommandEnum.PRELOAD_MAPPING == commandEnum) {
                reload(map, CommandEnum.PRELOAD_TABLE_GROUP, operationTemplate.getGroupId(model, GroupStrategyEnum.PRELOAD_TABLE_GROUP));
            }
        }
    }

    private void loadConnectorInstance() {
        List<Connector> list = profileComponent.getConnectorAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(connector->generalExecutor.execute(()-> {
                try {
                    ConnectorInstance connectorInstance = connectorFactory.connect(connector.getId(), connector.getConfig(), StringUtil.EMPTY, StringUtil.EMPTY);
                    logger.info("Completed connection {} {}", connector.getConfig().getConnectorType(), connectorInstance.getServiceUrl());
                } catch (Exception e) {
                    logger.error("连接配置异常", e);
                    logService.log(LogType.ConnectorLog.FAILED, e.getMessage());
                }
            }));
        }
    }

    /**
     * 恢复订正校验任务。
     * <p>任务配置在 {@code dbsyncer_task}，表映射在 {@code dbsyncer_table_group}；
     * TaskService 企业实现启动时已从库加载缓存，此处只做连接器预热与运行中任务续跑。
     */
    private void resumeValidateSyncTasks() {
        List<ConfigModel> taskAll = taskService.getTaskAll(CommonTaskTypeEnum.VALIDATE_SYNC);
        if (CollectionUtils.isEmpty(taskAll)) {
            return;
        }
        for (ConfigModel commonTask : taskAll) {
            if (!(commonTask instanceof ValidateSyncTask)) {
                continue;
            }
            ValidateSyncTask task = (ValidateSyncTask) commonTask;
            try {
                reConnect(task);
            } catch (Exception e) {
                logger.error("校验任务连接器预热失败, taskId={}, err={}", task.getId(), e.getMessage(), e);
            }
        }
        resumeRunningCommonTasks(taskAll);
    }

    /**
     * 恢复整库迁移任务。
     * <p>库表关联已下沉 {@code dbsyncer_table_group}，不再依赖任务 JSON 内 mappings；
     * 连接器在 Handler 启动时按 table_group 初始化，此处只续跑运行中任务。
     */
    private void resumeDatabaseSyncTasks() {
        List<ConfigModel> taskAll = taskService.getTaskAll(CommonTaskTypeEnum.DATABASE_SYNC);
        if (CollectionUtils.isEmpty(taskAll)) {
            return;
        }
        resumeRunningCommonTasks(taskAll);
    }

    /**
     * 将中断前 Meta.state=RUNNING 的任务重新拉起（先将 Meta 置 READY，再 start）。
     */
    private void resumeRunningCommonTasks(List<ConfigModel> taskAll) {
        for (ConfigModel task : taskAll) {
            if (task == null || StringUtil.isBlank(task.getId())) {
                continue;
            }
            Meta meta = metaProfile.getMetaByTaskId(task.getId(), TaskLevelEnum.TASK);
            if (meta == null || meta.getState() != MetaEnum.RUNNING.getCode()) {
                continue;
            }
            try {
                meta.setState(MetaEnum.READY.getCode());
                meta.setUpdateTime(System.currentTimeMillis());
                profileComponent.editConfigModel(meta);
                taskService.start(task.getId());
                logger.info("已恢复运行中任务: type={}, taskId={}, name={}", task.getType(), task.getId(), task.getName());
            } catch (Exception e) {
                logger.error("恢复任务失败, taskId={}, err={}", task.getId(), e.getMessage(), e);
            }
        }
    }
}