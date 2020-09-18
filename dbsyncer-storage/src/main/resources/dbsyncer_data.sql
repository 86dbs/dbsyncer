CREATE TABLE `dbsyncer_data` (
  `ID` varchar(64) COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `NAME` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '名称',
  `SUCCESS` varchar(6) COLLATE utf8_bin NOT NULL COMMENT '是否成功：true/false',
  `EVENT` varchar(255) COLLATE utf8_bin NOT NULL COMMENT '事件',
  `ERROR` varchar(1024) COLLATE utf8_bin DEFAULT NULL COMMENT '异常信息',
  `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
  `JSON` text COLLATE utf8_bin NOT NULL COMMENT '同步数据',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='同步数据表';