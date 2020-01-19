package org.dbsyncer.listener.mysql.binlog.impl.filter;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.binlog.BinlogParserContext;
import org.dbsyncer.listener.mysql.binlog.BinlogRowEventFilter;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h3>默认拦截器</h3>
 * <ol type="1">
 * <li><dt>自定义拦截</dt></li>
 * </ol>
 *
 * @ClassName: BinlogRowEventFilterImpl
 * @author: AE86
 * @date: 2018年10月15日 下午5:25:52
 */
public class BinlogRowEventFilterImpl implements BinlogRowEventFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogRowEventFilterImpl.class);

    private boolean verbose = true;

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean accepts(BinlogEventV4Header header, BinlogParserContext context, TableMapEvent event) {
        if (event == null) {
            if (isVerbose() && LOGGER.isWarnEnabled()) {
                LOGGER.warn("failed to find TableMapEvent, header: {}", header);
            }
            return false;
        }
        return true;
    }
}
