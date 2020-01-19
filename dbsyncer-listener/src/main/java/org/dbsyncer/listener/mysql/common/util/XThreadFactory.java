package org.dbsyncer.listener.mysql.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class XThreadFactory implements ThreadFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(XThreadFactory.class);

	protected String name;
	protected final AtomicBoolean daemon;
	protected final AtomicBoolean trackThreads;
	protected final List<WeakReference<Thread>> threads;
	protected final ConcurrentHashMap<String, AtomicLong> sequences;
	protected final AtomicReference<UncaughtExceptionHandler> uncaughtExceptionHandler;

	public XThreadFactory() {
		this(null, false, null);
	}

	public XThreadFactory(String name) {
		this(name, false, null);
	}

	public XThreadFactory(String name, boolean daemon) {
		this(name, daemon, null);
	}

	public XThreadFactory(String name, boolean daemon, UncaughtExceptionHandler handler) {
		this.name = name;
		this.daemon = new AtomicBoolean(daemon);
		this.trackThreads = new AtomicBoolean(false);
		this.threads = new LinkedList<WeakReference<Thread>>();
		this.sequences = new ConcurrentHashMap<String, AtomicLong>();
		this.uncaughtExceptionHandler = new AtomicReference<UncaughtExceptionHandler>(handler);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isDaemon() {
		return daemon.get();
	}

	public void setDaemon(boolean daemon) {
		this.daemon.set(daemon);
	}

	public UncaughtExceptionHandler getUncaughtExceptionHandler() {
		return uncaughtExceptionHandler.get();
	}

	public void setUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
		this.uncaughtExceptionHandler.set(handler);
	}

	public boolean isTrackThreads() {
		return trackThreads.get();
	}

	public void setTrackThreads(boolean trackThreads) {
		this.trackThreads.set(trackThreads);
	}

	public List<Thread> getAliveThreads() {
		return getThreads(true);
	}

	public Thread newThread(Runnable r) {
		final Thread t = new Thread(r);
		t.setDaemon(isDaemon());

		String prefix = this.name;
		if(prefix == null || prefix.equals("")) {
			prefix = getInvoker(2);
		}
		t.setName(prefix + "-" + getSequence(prefix));

		final UncaughtExceptionHandler handler = this.getUncaughtExceptionHandler();
		if(handler != null) {
			t.setUncaughtExceptionHandler(handler);
		} else {
			t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
				public void uncaughtException(Thread t, Throwable e) {
					LOGGER.error("unhandled exception in thread: " + t.getId() + ":" + t.getName(), e);
				}
			});
		}

		if(this.isTrackThreads()) {
			addThread(t);
		}

		return t;
	}

	protected String getInvoker(int depth) {
		final Exception e = new Exception();
		final StackTraceElement[] stes = e.getStackTrace();
		if(stes.length > depth) {
			return ClassUtils.getShortClassName(stes[depth].getClassName());
		} else {
			return getClass().getSimpleName();
		}
	}

	protected long getSequence(String invoker) {
		AtomicLong r = this.sequences.get(invoker);
		if(r == null) {
			r = new AtomicLong(0);
			AtomicLong existing = this.sequences.putIfAbsent(invoker, r);
			if(existing != null) {
				r = existing;
			}
		}
		return r.incrementAndGet();
	}

	protected synchronized void addThread(Thread thread) {
		for(Iterator<WeakReference<Thread>> iter = this.threads.iterator(); iter.hasNext(); ) {
			Thread t = iter.next().get();
			if(t == null) {
				iter.remove();
			}
		}

		this.threads.add(new WeakReference<Thread>(thread));
	}

	protected synchronized List<Thread> getThreads(boolean aliveOnly) {
		final List<Thread> r = new LinkedList<Thread>();
		for(Iterator<WeakReference<Thread>> iter = this.threads.iterator(); iter.hasNext(); ) {
			Thread t = iter.next().get();
			if(t == null) {
				iter.remove();
			} else if(!aliveOnly || t.isAlive()){
				r.add(t);
			}
		}
		return r;
	}
}
