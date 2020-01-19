package org.dbsyncer.listener.mysql.binlog;

public interface BinlogParserListener {

	void onStart(BinlogParser parser);

	void onStop(BinlogParser parser);

	void onException(BinlogParser parser, Exception eception);

	class Adapter implements BinlogParserListener {

		public void onStart(BinlogParser parser) {
		}

		public void onStop(BinlogParser parser) {
		}

		public void onException(BinlogParser parser, Exception exception) {
		}
	}
}
