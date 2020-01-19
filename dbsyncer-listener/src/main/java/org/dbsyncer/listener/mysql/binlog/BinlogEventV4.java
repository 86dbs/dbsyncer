package org.dbsyncer.listener.mysql.binlog;

/**
 * 
 * <h3>BinlogEventV4</h3>
 * <ol type="1">
 * 	<li><dt>事件类型</dt></li>
 *  <pre>
 *  +=====================================+
 * | event  | timestamp         0 : 4    |
 * | header +----------------------------+
 * |        | type_code         4 : 1    |
 * |        +----------------------------+
 * |        | server_id         5 : 4    |
 * |        +----------------------------+
 * |        | event_length      9 : 4    |
 * |        +----------------------------+
 * |        | next_position    13 : 4    |
 * |        +----------------------------+
 * |        | flags            17 : 2    |
 * +=====================================+
 * | event  | fixed part       19 : y    |
 * | data   +----------------------------+
 * |        | variable part              |
 * +=====================================+
 *  </pre>
 * </ol>
 * @ClassName: BinlogEventV4 
 * @author: AE86
 * @date: 2018年10月17日 下午2:21:28
 */
public interface BinlogEventV4 {

	BinlogEventV4Header getHeader();
}
