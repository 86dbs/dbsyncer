create TABLE `dbsyncer_task_database_sync_detail` (
    `ID`                  varchar(64)  NOT NULL comment '唯一ID',
    `TASK_ID`             varchar(64)  NOT NULL comment '迁移任务ID',
    `TYPE`                varchar(32)  NOT NULL comment '类型: tableSchema-结构迁移 rowData-数据迁移',
    `STATUS` tinyint NOT NULL DEFAULT '0' comment '执行状态, 0-运行中；1-已完成；',
    `TABLE_INDEX`         int          NOT NULL comment '库映射内表序号',
    `SOURCE_DATABASE`     varchar(64)  DEFAULT '' comment '源库名',
    `SOURCE_SCHEMA`       varchar(64)  DEFAULT '' comment '源Schema',
    `TARGET_DATABASE`     varchar(64)  DEFAULT '' comment '目标库名',
    `TARGET_SCHEMA`       varchar(64)  DEFAULT '' comment '目标Schema',
    `SOURCE_TABLE`        varchar(64)  DEFAULT '' comment '源表名',
    `TARGET_TABLE`        varchar(64)  DEFAULT '' comment '目标表名',
    `SOURCE_TOTAL`        bigint       DEFAULT NULL comment '源端总行数',
    `SUCCESS_TOTAL`       bigint       NOT NULL DEFAULT 0 comment '成功行数',
    `FAIL_TOTAL`          bigint       NOT NULL DEFAULT 0 comment '失败行数',
    `CONTENT` text CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL comment '对比结果 最高保存1000条数据',
    `CREATE_TIME`         bigint       NOT NULL comment '创建时间',
    `UPDATE_TIME`         bigint       NOT NULL comment '修改时间',
    PRIMARY KEY (`ID`),
    UNIQUE KEY `UK_TASK_TYPE_TABLE` (`TASK_ID`,`TYPE`,`SOURCE_DATABASE`,`SOURCE_SCHEMA`,`TARGET_DATABASE`,`TARGET_SCHEMA`,`TABLE_INDEX`),
    KEY `IDX_TASK_STATUS` (`TASK_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin comment='整库迁移任务明细表';

-- 已有库升级（按需执行）：
-- ALTER TABLE `dbsyncer_task_database_sync_detail` ADD COLUMN `TARGET_SCHEMA` varchar(64) DEFAULT '' COMMENT '目标Schema' AFTER `TARGET_DATABASE`;
-- ALTER TABLE `dbsyncer_task_database_sync_detail` DROP INDEX `UK_TASK_TYPE_TABLE`;
-- ALTER TABLE `dbsyncer_task_database_sync_detail` ADD UNIQUE KEY `UK_TASK_TYPE_TABLE` (`TASK_ID`,`TYPE`,`SOURCE_DATABASE`,`SOURCE_SCHEMA`,`TARGET_DATABASE`,`TARGET_SCHEMA`,`TABLE_INDEX`);
