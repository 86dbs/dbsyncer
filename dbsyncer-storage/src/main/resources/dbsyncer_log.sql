CREATE TABLE `dbsyncer_log` (
  `ID` varchar(64) COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `NAME` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '名称',
  `TYPE` varchar(24) COLLATE utf8_bin NOT NULL COMMENT '连接器、映射配置、表映射、元信息、系统日志',
  `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
  `JSON` text COLLATE utf8_bin NOT NULL COMMENT '日志信息',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='操作日志表';