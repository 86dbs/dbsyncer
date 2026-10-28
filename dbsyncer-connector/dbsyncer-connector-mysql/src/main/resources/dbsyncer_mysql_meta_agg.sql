CREATE TABLE `dbsyncer_meta_agg`
(
    `ID`          varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '唯一ID',
    `STAT_DATE`   varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '统计日期 yyyy-MM-dd',
    `TOTAL`       bigint                                                NOT NULL DEFAULT 0 COMMENT '同步任务total',
    `SUCCESS`     bigint                                                NOT NULL DEFAULT 0 COMMENT '同步任务success',
    `FAIL`        bigint                                                NOT NULL DEFAULT 0 COMMENT '同步任务fail',
    `CREATE_TIME` bigint                                                NOT NULL COMMENT '创建时间',
    `UPDATE_TIME` bigint                                                NOT NULL COMMENT '修改时间',
    PRIMARY KEY (`ID`) USING BTREE,
    UNIQUE KEY `UK_META_AGG_STAT_DATE` (`STAT_DATE`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='同步任务Meta聚合' ROW_FORMAT=Dynamic;
