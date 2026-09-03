CREATE TABLE `dbsyncer_task_plan` (
  `ID`              bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `TASK_ID`         varchar(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务ID',
  `TABLE_GROUP_ID`  varchar(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '表映射ID',
  `NODE_ID`         varchar(64)  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '执行节点，{ip}:{httpPort}',
  `START_CURSOR`    varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '分片起始游标',
  `END_CURSOR`      varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '分片结束游标',
  `STATUS`          tinyint NOT NULL DEFAULT 0 COMMENT '0-未完成,1-运行中,2-完成',
  `LAST_PAGE`       tinyint NOT NULL DEFAULT 0 COMMENT '是否表尾最后一页：0-否,1-是',
  `CREATE_TIME`     bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME`     bigint NOT NULL COMMENT '修改时间（运行中心跳/超时判定）',
  PRIMARY KEY (`ID`),
  KEY `IDX_TASK_TABLE` (`TASK_ID`, `TABLE_GROUP_ID`, `STATUS`),
  KEY `IDX_NODE_STATUS` (`NODE_ID`, `STATUS`),
  KEY `IDX_TABLE_STATUS` (`TABLE_GROUP_ID`, `STATUS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='任务分片计划表';
