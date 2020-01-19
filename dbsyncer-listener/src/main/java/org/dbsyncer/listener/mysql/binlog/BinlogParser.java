package org.dbsyncer.listener.mysql.binlog;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface BinlogParser {

	boolean isRunning();

	void start(String threadSuffixName) throws Exception;

	void stop(long timeout, TimeUnit unit) throws Exception;

	void setEventFilter(BinlogEventFilter filter);

	void setEventListener(BinlogEventListener listener);

	List<BinlogParserListener> getParserListeners();

	boolean addParserListener(BinlogParserListener listener);

	void removeParserListener();
	
	boolean removeParserListener(BinlogParserListener listener);

	void setParserListeners(List<BinlogParserListener> listeners);
}
