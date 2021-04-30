CREATE TABLE `dbsyncer_log`  (
  `ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `TYPE` varchar(24) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '连接器、映射配置、表映射、元信息、系统日志',
  `CREATE_TIME` bigint(0) NOT NULL COMMENT '创建时间',
  `JSON` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '日志信息',
  PRIMARY KEY (`ID`) USING BTREE,
  FULLTEXT INDEX `FULL_TEXT_JSON`(`JSON`)
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '操作日志表' ROW_FORMAT = Dynamic;