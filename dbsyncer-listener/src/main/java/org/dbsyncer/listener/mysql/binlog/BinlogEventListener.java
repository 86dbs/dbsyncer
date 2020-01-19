package org.dbsyncer.listener.mysql.binlog;

public interface BinlogEventListener {

	void onEvents(BinlogEventV4 event);
}
