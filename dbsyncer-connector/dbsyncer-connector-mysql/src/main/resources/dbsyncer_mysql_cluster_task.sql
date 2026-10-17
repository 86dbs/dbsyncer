CREATE TABLE `dbsyncer_cluster_task` (
  `ID`                bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `CREATE_TIME`       bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME`       bigint NOT NULL COMMENT '修改时间',
  `TASK_ID`           varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务ID',
  `TASK_TYPE`         varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务类型',
  `NODE_ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '被分配的调度节点',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `UK_TASK` (`TASK_ID`),
  KEY `IDX_SCHEDULER` (`NODE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='集群任务调度表';
