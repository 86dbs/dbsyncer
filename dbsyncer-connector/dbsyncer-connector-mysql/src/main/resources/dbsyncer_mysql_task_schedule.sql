CREATE TABLE `dbsyncer_task_schedule` (
  `ID`                bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `TASK_ID`           varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务ID',
  `TASK_TYPE`         varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务类型',
  `INITIATOR_NODE_ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '启动任务的节点',
  `SCHEDULER_NODE_ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '被分配的调度节点，停止时置空',
  `SCHEDULER_EPOCH`   int NOT NULL DEFAULT 1 COMMENT '调度代数',
  `SCHEDULER_START_TIME` bigint NOT NULL DEFAULT 0 COMMENT '调度节点 ACK 时间；0 表示尚未接管',
  `ERROR_MSG`         varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '分配失败原因',
  `CREATE_TIME`       bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME`       bigint NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `UK_TASK` (`TASK_ID`),
  KEY `IDX_SCHEDULER` (`SCHEDULER_NODE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='任务调度表';
