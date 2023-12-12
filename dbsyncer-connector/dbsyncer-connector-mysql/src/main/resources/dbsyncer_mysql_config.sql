CREATE TABLE `dbsyncer_config`  (
  `ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `NAME` varchar(50) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '名称',
  `TYPE` varchar(24) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT 'system、user、connector、mapping、tableGroup、meta、projectGroup',
  `CREATE_TIME` bigint(0) NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint(0) NOT NULL COMMENT '修改时间',
  `JSON` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '配置信息',
  PRIMARY KEY (`ID`) USING BTREE,
  INDEX `IDX_TYPE_UPDATE_CREATE_TIME`(`TYPE`, `UPDATE_TIME`, `CREATE_TIME`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '配置信息表' ROW_FORMAT = Dynamic;