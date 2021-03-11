CREATE TABLE `dbsyncer_data` (
  `ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `SUCCESS` int(1) NOT NULL COMMENT '成功1/失败0',
  `EVENT` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '事件',
  `ERROR` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NULL COMMENT '异常信息',
  `CREATE_TIME` bigint(0) NOT NULL COMMENT '创建时间',
  `JSON` mediumtext CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '同步数据',
  PRIMARY KEY (`ID`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '同步数据表' ROW_FORMAT = Dynamic;