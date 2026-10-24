CREATE TABLE `dbsyncer_notice_strategy` (
  `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '唯一ID',
  `CREATE_TIME` bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint NOT NULL COMMENT '修改时间',
  `NAME` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '策略名称',
  `NOTICE_TYPES` int NOT NULL COMMENT '通知类型码，存 NoticeTypeEnum.code',
  `ENABLED` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否启用：0否1是',
  `FREQUENCY` tinyint NOT NULL COMMENT '频率：0-IMMEDIATE;1-INTERVAL',
  `CRON` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '周期Cron，立即策略可空',
  `REMARK` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '备注说明',
  PRIMARY KEY (`ID`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='告警策略表' ROW_FORMAT=Dynamic;
