CREATE TABLE `dbsyncer_task_detail` (
  `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '唯一ID',
  `CREATE_TIME` bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint NOT NULL COMMENT '修改时间',
  `TABLE_GROUP_ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '关联表映射ID',
  `TYPE` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务类型, 事件insert/update/delete/ddl; 校验/迁移',
  `TARGET_TABLE` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT '' COMMENT '目标表名称',
  `IS_SUCCESS` tinyint NOT NULL DEFAULT 0 COMMENT '是否成功/状态, 0-失败或运行中 1-成功或完成',
  `ERROR` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '异常报错信息',
  `DATA` blob DEFAULT NULL COMMENT '明细数据',
  PRIMARY KEY (`ID`) USING BTREE,
  KEY `IDX_TABLE_GROUP_ID` (`TABLE_GROUP_ID`) USING BTREE,
  KEY `IDX_SUCCESS_CREATE_TIME` (`IS_SUCCESS`, `CREATE_TIME`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '任务明细表' ROW_FORMAT = Dynamic;
