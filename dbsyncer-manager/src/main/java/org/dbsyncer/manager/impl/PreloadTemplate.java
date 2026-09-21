/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.manager.impl;

import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
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
import org.dbsyncer.sdk.service.ScheduledScanManager;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

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

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private PluginFactory pluginFactory;

    @Resource
    private MessageService messageService;

    @Resource
    private LogService logService;

    @Resource
    private Executor generalExecutor;

    private boolean preloadCompleted;

    @Resource
    private TaskService<ConfigModel> taskService;

    @Resource
    private ClusterService clusterService;

    @Resource
    private ScheduledScanManager scheduledScanManager;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        // Load plugins
        pluginFactory.loadPlugins();

        // Load Notification Channels
        loadNotificationChannel();

        if (clusterService.isStandalone()) {
            // Load connectorInstances
            loadConnectorInstance();
            // 同步驱动：按任务级 Meta 恢复 Mapping
            launchSyncMappings();
            // 订正校验 / 整库迁移
            resumeValidateSyncTasks();
            resumeDatabaseSyncTasks();
            //初始化定时检测任务
            scheduledScanManager.start();
        } else {
            // 集群：不预热全部连接；由控制面按本机负责任务先恢复连接再启任务
            clusterService.init();
        }
        preloadCompleted = true;
    }

    public void loadNotificationChannel() {
        try {
            SystemConfig systemConfig = initSystemConfigIfAbsent();
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
     * 启动时若无系统配置则写入默认行，避免后续 getSystemConfig() 为空 NPE。
     *
     * @return 已有或新建的系统配置
     */
    private SystemConfig initSystemConfigIfAbsent() {
        SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
        if (systemConfig != null) {
            return systemConfig;
        }
        systemConfig = new SystemConfig();
        systemConfig.setName("系统配置");
        long now = System.currentTimeMillis();
        systemConfig.setCreateTime(now);
        systemConfig.setUpdateTime(now);
        systemConfigProfile.saveSystemConfig(systemConfig);
        logger.warn("No system config found, created default system config");
        return systemConfig;
    }

    /**
     * 是否完成预加载配置
     */
    public boolean isPreloadCompleted() {
        return preloadCompleted;
    }

    /**
     * 配置导入完成后的收尾：重建连接实例，恢复同步驱动与企业任务。
     */
    public void afterConfigImport() {
        // 集群不预热全部连接器；单机仍全量预热
        if (clusterService.isStandalone()) {
            loadConnectorInstance();
        }
        launchSyncMappings();
        resumeValidateSyncTasks();
        resumeDatabaseSyncTasks();
    }

    /**
     * 恢复同步驱动(Mapping)。
     * <p>先按任务类型 {@code mapping} 分页拉取任务，再批量查任务级 Meta（{@code isTaskDetail=0}），
     * 避免一次性加载全部 Meta。明细级 Meta 属于校验/迁移结果或表级进度，不参与驱动启停。
     */
    private void launchSyncMappings() {
        taskProfile.pageScanTasks(Mapping.class, ConfigConstant.PAGE_SIZE, mappings -> {
            if (CollectionUtils.isEmpty(mappings)) {
                return;
            }
            List<String> taskIds = new ArrayList<>();
            for (Mapping mapping : mappings) {
                if (mapping != null && StringUtil.isNotBlank(mapping.getId())) {
                    taskIds.add(mapping.getId());
                }
            }
            Map<String, Meta> metaMap = metaProfile.getTaskMetaMap(taskIds);
            if (CollectionUtils.isEmpty(metaMap)) {
                return;
            }
            for (Mapping mapping : mappings) {
                if (mapping == null || StringUtil.isBlank(mapping.getId())) {
                    continue;
                }
                Meta meta = metaMap.get(mapping.getId());
                if (meta == null) {
                    continue;
                }
                try {
                    reConnect(mapping);
                    // 恢复驱动状态（自动恢复：CDC 监听启动失败时按配置重试）
                    if (CommonTaskStatusEnum.RUNNING.getCode() == meta.getState()) {
                        clusterService.start(mapping, true);
                    } else if (CommonTaskStatusEnum.STOPPING.getCode() == meta.getState()) {
                        changeMetaState(meta.getId(), CommonTaskStatusEnum.READY);
                    }
                } catch (Exception e) {
                    logger.error("恢复同步驱动失败, metaId={}, taskId={}, err={}", meta.getId(), mapping.getId(), e.getMessage(), e);
                }
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
        Connector connector = connectorProfile.getConnector(sourceConnectorId);
        ConnectorInstance instance = connectorFactory.connect(sourceInstanceId, connector.getConfig(), sourceDatabase, sourceSchema);
        Assert.notNull(instance, "Source connector instance can not null");
        connector = connectorProfile.getConnector(targetConnectorId);
        instance = connectorFactory.connect(targetInstanceId, connector.getConfig(), targetDatabase, targetSchema);
        Assert.notNull(instance, "Target connector instance can not null");
    }

    private void loadConnectorInstance() {
        List<Connector> list = connectorProfile.getConnectorAll();
        if (!CollectionUtils.isEmpty(list)) {
            list.forEach(connector -> generalExecutor.execute(() -> {
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
            if (meta == null || meta.getState() != CommonTaskStatusEnum.RUNNING.getCode()) {
                continue;
            }
            try {
                meta.setState(CommonTaskStatusEnum.READY.getCode());
                meta.setUpdateTime(System.currentTimeMillis());
                metaProfile.updateMeta(meta);
                taskService.start(task.getId());
                logger.info("已恢复运行中任务: type={}, taskId={}, name={}", task.getType(), task.getId(), task.getName());
            } catch (Exception e) {
                logger.error("恢复任务失败, taskId={}, err={}", task.getId(), e.getMessage(), e);
            }
        }
    }

    private void changeMetaState(String metaId, CommonTaskStatusEnum status) {
        Meta meta = metaProfile.getMeta(metaId);
        int code = status.getCode();
        if (null != meta && meta.getState() != code) {
            long now = Instant.now().toEpochMilli();
            meta.setState(code);
            meta.setUpdateTime(now);
            // 进入运行中时记录本轮启动时间，供耗时（updateTime - startTime）计算
            if (CommonTaskStatusEnum.RUNNING == status) {
                meta.setStartTime(now);
            }
            metaProfile.updateMeta(meta);
        }
    }
}