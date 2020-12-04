package org.dbsyncer.listener.mysql;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.ServerException;

import java.util.Map;

public interface BinaryLogClient {

    /**
     * Connect to the replication stream in a separate thread.
     *
     * @throws AuthenticationException if authentication fails
     * @throws ServerException         if MySQL server responds with an error
     * @throws Exception               if anything goes wrong while trying to connect
     */
    void connect() throws Exception;

    /**
     * Disconnect from the replication stream. Note that this does not reset binlogFilename/binlogPosition. Calling {@link #connect(int)}}
     * again resumes client from where it left off.
     */
    void disconnect() throws Exception;

    /**
     * @return true if client is connected, false otherwise
     */
    boolean isConnected();

    /**
     * Register event listener. Note that multiple event listeners will be called in order they where registered.
     */
    void registerEventListener(BinaryLogRemoteClient.EventListener eventListener);

    /**
     * Register lifecycle listener. Note that multiple lifecycle listeners will be called in order they where registered.
     */
    void registerLifecycleListener(BinaryLogRemoteClient.LifecycleListener lifecycleListener);

    /**
     * @return binary log filename, nullable (and null be default). Note that this value is automatically tracked by the client and thus is
     * subject to change (in response to {@link EventType#ROTATE}, for example).
     * @see #setBinlogFilename(String)
     */
    String getBinlogFilename();

    /**
     * @param binlogFilename binary log filename. Special values are:
     *                       <ul>
     *                       <li>null, which turns on automatic resolution (resulting in the last known binlog and position). This is what
     *                       happens by default when you don't specify binary log filename explicitly.</li>
     *                       <li>"" (empty string), which instructs server to stream events starting from the oldest known binlog.</li>
     *                       </ul>
     * @see #getBinlogFilename()
     */
    void setBinlogFilename(String binlogFilename);

    /**
     * @return binary log position of the next event, 4 by default (which is a position of first event). Note that this value changes with
     * each incoming event.
     * @see #setBinlogPosition(long)
     */
    long getBinlogPosition();

    /**
     * @param binlogPosition binary log position. Any value less than 4 gets automatically adjusted to 4 on connect.
     * @see #getBinlogPosition()
     */
    void setBinlogPosition(long binlogPosition);

    /**
     * @return event deserializer
     * @see #setEventDeserializer(EventDeserializer)
     */
    EventDeserializer getEventDeserializer();

    /**
     * @param eventDeserializer custom event deserializer
     */
    void setEventDeserializer(EventDeserializer eventDeserializer);

    /**
     * @return tableMapEventByTableId
     */
    Map<Long, TableMapEventData> getTableMapEventByTableId();

    /**
     * @param tableMapEventByTableId tableMapEventMetadata
     */
    void setTableMapEventByTableId(Map<Long, TableMapEventData> tableMapEventByTableId);

    /**
     * SimpleEventModel
     *
     * @return
     */
    boolean isSimpleEventModel();

    /**
     * <p>true: ROTATE > FORMAT_DESCRIPTION > TABLE_MAP > WRITE_ROWS > UPDATE_ROWS > DELETE_ROWS > XID
     * <p>false: Support all events
     *
     * @param simpleEventModel
     */
    void setSimpleEventModel(boolean simpleEventModel);

    /**
     * binlog-parser-127.0.0.1_3306_1
     *
     * @return workerThreadName
     */
    String getWorkerThreadName();

}