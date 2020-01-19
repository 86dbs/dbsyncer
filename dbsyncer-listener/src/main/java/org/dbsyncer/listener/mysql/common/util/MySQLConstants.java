package org.dbsyncer.listener.mysql.common.util;

public final class MySQLConstants {
	public static final byte[] BINLOG_MAGIC = new byte[]{(byte)0xfe, (byte)0x62, (byte)0x69, (byte)0x6e};

	public static final int MAX_TIME_WIDTH = 10;
	public static final int MAX_DATETIME_WIDTH = 19;
	public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);

	// Client capabilities
	public static final int CLIENT_LONG_PASSWORD = 1; /* new more secure passwords */
	public static final int CLIENT_FOUND_ROWS = 2; /* Found instead of affected rows */
	public static final int CLIENT_LONG_FLAG = 4; /* Get all column flags */
	public static final int CLIENT_CONNECT_WITH_DB = 8; /* One can specify db on connect */
	public static final int CLIENT_NO_SCHEMA = 16; /* Don't allow database.table.column */
	public static final int CLIENT_COMPRESS = 32; /* Can use compression protocol */
	public static final int CLIENT_ODBC = 64; /* Odbc client */
	public static final int CLIENT_LOCAL_FILES = 128; /* Can use LOAD DATA LOCAL */
	public static final int CLIENT_IGNORE_SPACE = 256; /* Ignore spaces before '' */
	public static final int CLIENT_PROTOCOL_41 = 512; /* New 4.1 protocol */
	public static final int CLIENT_INTERACTIVE = 1024; /* This is an interactive client */
	public static final int CLIENT_SSL = 2048; /* Switch to SSL after handshake */
	public static final int CLIENT_IGNORE_SIGPIPE = 4096; /* IGNORE sigpipes */
	public static final int CLIENT_TRANSACTIONS = 8192; /* Client knows about transactions */
	public static final int CLIENT_RESERVED = 16384; /* Old flag for 4.1 protocol  */
	public static final int CLIENT_SECURE_CONNECTION = 32768; /* New 4.1 authentication */
	public static final int CLIENT_MULTI_STATEMENTS = 1 << 16; /* Enable/disable multi-stmt support */
	public static final int CLIENT_MULTI_RESULTS = 1 << 17; /* Enable/disable multi-results */
	public static final int CLIENT_PS_MULTI_RESULTS = 1 << 18; /* Multi-results in PS-protocol */
	public static final int CLIENT_PLUGIN_AUTH = 1 << 19; /* Client supports plugin authentication */
	public static final int CLIENT_SSL_VERIFY_SERVER_CERT = 1 << 30;
	public static final int CLIENT_REMEMBER_OPTIONS = 1 << 31;

	// Command
	public static final int COM_SLEEP = 0x00;
	public static final int COM_QUIT = 0x01;
	public static final int COM_INIT_DB = 0x02;
	public static final int COM_QUERY = 0x03;
	public static final int COM_FIELD_LIST = 0x04;
	public static final int COM_CREATE_DB = 0x05;
	public static final int COM_DROP_DB = 0x06;
	public static final int COM_REFRESH = 0x07;
	public static final int COM_SHUTDOWN = 0x08;
	public static final int COM_STATISTICS = 0x09;
	public static final int COM_PROCESS_INFO = 0x0a;
	public static final int COM_CONNECT = 0x0b;
	public static final int COM_PROCESS_KILL = 0x0c;
	public static final int COM_DEBUG = 0x0d;
	public static final int COM_PING = 0x0e;
	public static final int COM_TIME = 0x0f;
	public static final int COM_DELAYED_INSERT = 0x10;
	public static final int COM_CHANGE_USER = 0x11;
	public static final int COM_BINLOG_DUMP = 0x12;
	public static final int COM_TABLE_DUMP = 0x13;
	public static final int COM_CONNECT_OUT = 0x14;
	public static final int COM_REGISTER_SLAVE = 0x15;
	public static final int COM_STMT_PREPARE = 0x16;
	public static final int COM_STMT_EXECUTE = 0x17;
	public static final int COM_STMT_SEND_LONG_DATA = 0x18;
	public static final int COM_STMT_CLOSE = 0x19;
	public static final int COM_STMT_RESET = 0x1a;
	public static final int COM_SET_OPTION = 0x1b;
	public static final int COM_STMT_FETCH = 0x1c;
	public static final int COM_DAEMON = 0x1d;
	public static final int COM_END = 0x1e;

	// Status variable type
	public static final int Q_FLAGS2_CODE = 0;
	public static final int Q_SQL_MODE_CODE = 1;
	public static final int Q_CATALOG_CODE = 2;
	public static final int Q_AUTO_INCREMENT = 3;
	public static final int Q_CHARSET_CODE = 4;
	public static final int Q_TIME_ZONE_CODE = 5;
	public static final int Q_CATALOG_NZ_CODE = 6;
	public static final int Q_LC_TIME_NAMES_CODE = 7;
	public static final int Q_CHARSET_DATABASE_CODE = 8;
	public static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;
	public static final int Q_MASTER_DATA_WRITTEN_CODE = 10;
	public static final int Q_INVOKER = 11;
	public static final int Q_UPDATED_DB_NAMES = 12;
	public static final int Q_MICROSECONDS = 13;

	public static final int MAX_DBS_IN_EVENT_MTS = 16;
	public static final int OVER_MAX_DBS_IN_EVENT_MTS = 254;

	// User variable type
	public static final int STRING_RESULT = 0;
	public static final int REAL_RESULT = 1;
	public static final int INT_RESULT = 2;
	public static final int ROW_RESULT =3;
	public static final int DECIMAL_RESULT = 4;

	// Column type
	public static final int TYPE_DECIMAL = 0;
	public static final int TYPE_TINY = 1;
	public static final int TYPE_SHORT = 2;
	public static final int TYPE_LONG = 3;
	public static final int TYPE_FLOAT = 4;
	public static final int TYPE_DOUBLE = 5;
	public static final int TYPE_NULL = 6;
	public static final int TYPE_TIMESTAMP = 7;
	public static final int TYPE_LONGLONG = 8;
	public static final int TYPE_INT24 = 9;
	public static final int TYPE_DATE = 10;
	public static final int TYPE_TIME = 11;
	public static final int TYPE_DATETIME = 12;
	public static final int TYPE_YEAR = 13;
	public static final int TYPE_NEWDATE = 14;
	public static final int TYPE_VARCHAR = 15;
	public static final int TYPE_BIT = 16;
	public static final int TYPE_TIMESTAMP2 = 17;
	public static final int TYPE_DATETIME2 = 18;
	public static final int TYPE_TIME2 = 19;
	public static final int TYPE_NEWDECIMAL = 246;
	public static final int TYPE_ENUM = 247;
	public static final int TYPE_SET = 248;
	public static final int TYPE_TINY_BLOB = 249;
	public static final int TYPE_MEDIUM_BLOB = 250;
	public static final int TYPE_LONG_BLOB = 251;
	public static final int TYPE_BLOB = 252;
	public static final int TYPE_VAR_STRING = 253;
	public static final int TYPE_STRING = 254;
	public static final int TYPE_GEOMETRY = 255;

	// SQL modes
	public static final long SM_LREAL_AS_FLOATL = 0x1L;
	public static final long SM_LPIPES_AS_CONCATL = 0x2L;
	public static final long SM_LANSI_QUOTESL = 0x4L;
	public static final long SM_LIGNORE_SPACEL = 0x8L;
	public static final long SM_LNOT_USEDL = 0x10L;
	public static final long SM_LONLY_FULL_GROUP_BYL = 0x20L;
	public static final long SM_LNO_UNSIGNED_SUBTRACTIONL = 0x40L;
	public static final long SM_LNO_DIR_IN_CREATEL = 0x80L;
	public static final long SM_LPOSTGRESQLL = 0x100L;
	public static final long SM_LORACLEL = 0x200L;
	public static final long SM_LMSSQLL = 0x400L;
	public static final long SM_LDB2L = 0x800L;
	public static final long SM_LMAXDBL = 0x1000L;
	public static final long SM_LNO_KEY_OPTIONSL = 0x2000L;
	public static final long SM_LNO_TABLE_OPTIONSL = 0x4000L;
	public static final long SM_LNO_FIELD_OPTIONSL = 0x8000L;
	public static final long SM_LMYSQL323L = 0x10000L;
	public static final long SM_LMYSQL40L = 0x20000L;
	public static final long SM_LANSIL = 0x40000L;
	public static final long SM_LNO_AUTO_VALUE_ON_ZEROL = 0x80000L;
	public static final long SM_LNO_BACKSLASH_ESCAPESL = 0x100000L;
	public static final long SM_LSTRICT_TRANS_TABLESL = 0x200000L;
	public static final long SM_LSTRICT_ALL_TABLESL = 0x400000L;
	public static final long SM_LNO_ZERO_IN_DATEL = 0x800000L;
	public static final long SM_LNO_ZERO_DATEL = 0x1000000L;
	public static final long SM_LINVALID_DATESL = 0x2000000L;
	public static final long SM_LERROR_FOR_DIVISION_BY_ZEROL = 0x4000000L;
	public static final long SM_LTRADITIONALL = 0x8000000L;
	public static final long SM_LNO_AUTO_CREATE_USERL = 0x10000000L;
	public static final long SM_LHIGH_NOT_PRECEDENCEL = 0x20000000L;
	public static final long SM_LNO_ENGINE_SUBSTITUTIONL = 0x40000000L;
	public static final long SM_LPAD_CHAR_TO_FULL_LENGTHL = 0x80000000L;

	// sql/log_event.h
	public static final int UNKNOWN_EVENT = 0;
	public static final int START_EVENT_V3 = 1;
	public static final int QUERY_EVENT = 2;
	public static final int STOP_EVENT = 3;
	public static final int ROTATE_EVENT = 4;
	public static final int INTVAR_EVENT = 5;
	public static final int LOAD_EVENT = 6;
	public static final int SLAVE_EVENT = 7;
	public static final int CREATE_FILE_EVENT = 8;
	public static final int APPEND_BLOCK_EVENT = 9;
	public static final int EXEC_LOAD_EVENT = 10;
	public static final int DELETE_FILE_EVENT = 11;
	public static final int NEW_LOAD_EVENT = 12;
	public static final int RAND_EVENT = 13;
	public static final int USER_VAR_EVENT = 14;
	public static final int FORMAT_DESCRIPTION_EVENT = 15;
	public static final int XID_EVENT = 16;
	public static final int BEGIN_LOAD_QUERY_EVENT = 17;
	public static final int EXECUTE_LOAD_QUERY_EVENT = 18;
	public static final int TABLE_MAP_EVENT = 19;
	public static final int PRE_GA_WRITE_ROWS_EVENT = 20;
	public static final int PRE_GA_UPDATE_ROWS_EVENT = 21;
	public static final int PRE_GA_DELETE_ROWS_EVENT = 22;
	public static final int WRITE_ROWS_EVENT = 23;
	public static final int UPDATE_ROWS_EVENT = 24;
	public static final int DELETE_ROWS_EVENT = 25;
	public static final int INCIDENT_EVENT = 26;
	public static final int HEARTBEAT_LOG_EVENT = 27;
	public static final int IGNORABLE_LOG_EVENT = 28;
	public static final int ROWS_QUERY_LOG_EVENT = 29;
	public static final int WRITE_ROWS_EVENT_V2 = 30;
	public static final int UPDATE_ROWS_EVENT_V2 = 31;
	public static final int DELETE_ROWS_EVENT_V2 = 32;
	public static final int GTID_LOG_EVENT = 33;
	public static final int ANONYMOUS_GTID_LOG_EVENT = 34;
	public static final int PREVIOUS_GTIDS_LOG_EVENT = 35;
}
