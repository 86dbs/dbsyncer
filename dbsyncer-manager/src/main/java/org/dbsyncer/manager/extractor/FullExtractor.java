package org.dbsyncer.manager.extractor;

import org.dbsyncer.parser.Parser;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 全量同步
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/26 15:28
 */
@Component
public class FullExtractor extends AbstractExtractor {

    @Autowired
    private Parser parser;

    @Override
    protected void doTask(Mapping mapping) {
        // 获取数据源连接配置

        // 获取执行命令
    }

}