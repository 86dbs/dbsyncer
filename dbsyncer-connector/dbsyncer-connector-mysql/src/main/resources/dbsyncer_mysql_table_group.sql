CREATE TABLE `dbsyncer_table_group` (
  `ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `NAME` varchar(50) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '名称',
  `TYPE` varchar(24) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '配置类型(tableGroup)',
  `TASK_ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '关联驱动映射关系ID',
  `SORT_INDEX` int NOT NULL DEFAULT 0 COMMENT '排序序号',
  `CREATE_TIME` bigint(0) NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint(0) NOT NULL COMMENT '修改时间',
  `JSON` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '字段映射、过滤条件等完整配置JSON',
  PRIMARY KEY (`ID`) USING BTREE,
  KEY `IDX_TASK_SORT` (`TASK_ID`, `SORT_INDEX`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '表映射关系配置表' ROW_FORMAT = Dynamic;