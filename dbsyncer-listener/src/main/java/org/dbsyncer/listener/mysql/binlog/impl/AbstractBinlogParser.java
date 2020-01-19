package org.dbsyncer.listener.mysql.binlog.impl;

import org.dbsyncer.listener.mysql.binlog.*;
import org.dbsyncer.listener.mysql.binlog.impl.event.RotateEvent;
import org.dbsyncer.listener.mysql.binlog.impl.event.TableMapEvent;
import org.dbsyncer.listener.mysql.binlog.impl.parser.NopEventParser;
import org.dbsyncer.listener.mysql.common.util.XThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractBinlogParser implements BinlogParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBinlogParser.class);

    protected Thread worker;
    protected String threadSuffixName;
    protected BinlogEventFilter eventFilter;
    protected BinlogEventListener eventListener;
    protected boolean clearTableMapEventsOnRotate = true;

    protected final List<BinlogParserListener> parserListeners = new CopyOnWriteArrayList<BinlogParserListener>();
    protected final AtomicBoolean verbose = new AtomicBoolean(false);
    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected final BinlogEventParser defaultParser = new NopEventParser();
    protected final BinlogEventParser[] parsers = new BinlogEventParser[128];
    protected String binlogFileName;

    protected abstract void doParse() throws Exception;

    protected abstract void doStart() throws Exception;

    protected abstract void doStop(long timeout, TimeUnit unit) throws Exception;

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    @Override
    public void start(String threadSuffixName) throws Exception {
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        doStart();

        if (null == threadSuffixName) {
            threadSuffixName = "binlog-parser";
            this.worker = new XThreadFactory(threadSuffixName, false).newThread(new Task());
        } else {
            this.worker = new Thread(new Task());
            this.worker.setDaemon(false);
            this.worker.setName(threadSuffixName);
        }
        this.worker.start();
        notifyOnStart();
    }

    @Override
    public void stop(long timeout, TimeUnit unit) throws Exception {
        if (!this.running.compareAndSet(true, false)) {
            return;
        }

        try {
            final long now = System.nanoTime();
            doStop(timeout, unit);
            timeout -= unit.convert(System.nanoTime() - now, TimeUnit.NANOSECONDS);

            if (timeout > 0) {
                unit.timedJoin(this.worker, timeout);
                this.worker = null;
            }
        } finally {
            notifyOnStop();
        }
    }

    public boolean isVerbose() {
        return this.verbose.get();
    }

    public void setVerbose(boolean verbose) {
        this.verbose.set(verbose);
    }

    public BinlogEventFilter getEventFilter() {
        return eventFilter;
    }

    @Override
    public void setEventFilter(BinlogEventFilter filter) {
        this.eventFilter = filter;
    }

    public BinlogEventListener getEventListener() {
        return eventListener;
    }

    @Override
    public void setEventListener(BinlogEventListener listener) {
        this.eventListener = listener;
    }

    public boolean isClearTableMapEventsOnRotate() {
        return clearTableMapEventsOnRotate;
    }

    public void setClearTableMapEventsOnRotate(boolean clearTableMapEventsOnRotate) {
        this.clearTableMapEventsOnRotate = clearTableMapEventsOnRotate;
    }

    public void clearEventParsers() {
        for (int i = 0; i < this.parsers.length; i++) {
            this.parsers[i] = null;
        }
    }

    public BinlogEventParser getEventParser(int type) {
        return this.parsers[type];
    }

    public BinlogEventParser unregisterEventParser(int type) {
        return this.parsers[type] = null;
    }

    public void registerEventParser(BinlogEventParser parser) {
        this.parsers[parser.getEventType()] = parser;
    }

    // maintain backwards compat
    @Deprecated
    public void registgerEventParser(BinlogEventParser parser) {
        this.registerEventParser(parser);
    }

    @Deprecated
    public BinlogEventParser unregistgerEventParser(int type) {
        return unregisterEventParser(type);
    }

    public void setEventParsers(List<BinlogEventParser> parsers) {
        clearEventParsers();
        if (parsers != null) {
            for (BinlogEventParser parser : parsers) {
                registerEventParser(parser);
            }
        }
    }

    @Override
    public List<BinlogParserListener> getParserListeners() {
        return new ArrayList<BinlogParserListener>(this.parserListeners);
    }

    @Override
    public boolean addParserListener(BinlogParserListener listener) {
        return this.parserListeners.add(listener);
    }

    @Override
    public void removeParserListener() {
        this.parserListeners.clear();
    }

    @Override
    public boolean removeParserListener(BinlogParserListener listener) {
        return this.parserListeners.remove(listener);
    }

    @Override
    public void setParserListeners(List<BinlogParserListener> listeners) {
        this.parserListeners.clear();
        if (listeners != null) this.parserListeners.addAll(listeners);
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    private void notifyOnStart() {
        for (BinlogParserListener listener : this.parserListeners) {
            listener.onStart(this);
        }
    }

    private void notifyOnStop() {
        for (BinlogParserListener listener : this.parserListeners) {
            listener.onStop(this);
        }
    }

    private void notifyOnException(Exception exception) {
        for (BinlogParserListener listener : this.parserListeners) {
            listener.onException(this, exception);
        }
    }

    protected class Task implements Runnable {
        public void run() {
            try {
                doParse();
            } catch (EOFException e) {
            } catch (Exception e) {
                notifyOnException(e);
            } finally {
                try {
                    stop(0, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LOGGER.error("failed to stop binlog parser", e);
                }
            }
        }
    }

    protected class Context implements BinlogParserContext, BinlogEventListener {
        private String binlogFileName;
        private final Map<Long, TableMapEvent> tableMapEvents = new HashMap<Long, TableMapEvent>();
        private boolean checksumEnabled;

        public Context(AbstractBinlogParser parser) {
            this.binlogFileName = parser.getBinlogFileName();

        }

        public final String getBinlogFileName() {
            return binlogFileName;
        }

        public final void setBinlogFileName(String name) {
            this.binlogFileName = name;
        }

        public final BinlogEventListener getEventListener() {
            return this;
        }

        public final TableMapEvent getTableMapEvent(long tableId) {
            return this.tableMapEvents.get(tableId);
        }

        public void onEvents(BinlogEventV4 event) {
            if (event == null) {
                return;
            }

            if (event instanceof TableMapEvent) {
                final TableMapEvent tme = (TableMapEvent) event;
                this.tableMapEvents.put(tme.getTableId(), tme);
            } else if (event instanceof RotateEvent) {
                final RotateEvent re = (RotateEvent) event;
                this.binlogFileName = re.getBinlogFileName().toString();
                if (isClearTableMapEventsOnRotate()) this.tableMapEvents.clear();
            }

            try {
                AbstractBinlogParser.this.eventListener.onEvents(event);
            } catch (Exception e) {
                LOGGER.error("failed to notify binlog event listener, event: " + event, e);
            }
        }

        public boolean getChecksumEnabled() {
            return this.checksumEnabled;
        }

        public void setChecksumEnabled(boolean flag) {
            this.checksumEnabled = flag;
        }
    }
}
