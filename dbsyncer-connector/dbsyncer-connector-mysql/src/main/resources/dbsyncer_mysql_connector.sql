CREATE TABLE `dbsyncer_connector` (
  `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '唯一ID',
  `CREATE_TIME` bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint NOT NULL COMMENT '修改时间',
  `NAME` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '连接器名称',
  `TYPE` varchar(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '连接类型',
  `IS_SOURCE` tinyint NOT NULL DEFAULT 1 COMMENT '作为源标识, 0-否 1-是',
  `IS_TARGET` tinyint NOT NULL DEFAULT 1 COMMENT '作为目标标识, 0-否 1-是',
  `JSON` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '配置信息',
  PRIMARY KEY (`ID`) USING BTREE,
  UNIQUE KEY `UK_NAME` (`NAME`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '连接配置表' ROW_FORMAT = Dynamic;
