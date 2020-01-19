package org.dbsyncer.listener.mysql.common.glossary.column;

import java.sql.Timestamp;

public final class Datetime2Column extends AbstractDatetimeColumn {
	private static final long serialVersionUID = 6444968242222031354L;

	private final java.util.Date value;

	private Datetime2Column(java.util.Date value) {
		this.value = value;
		this.timestampValue = new Timestamp(value.getTime());
	}

	private Datetime2Column(Timestamp value) {
		this.timestampValue = value;
		this.value = (java.util.Date) value;
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	public java.util.Date getValue() {
		return this.value;
	}

	public static final Datetime2Column valueOf(java.util.Date value) {
		return new Datetime2Column(value);
	}

	public static final Datetime2Column valueOf(Timestamp value) {
		return new Datetime2Column(value);
	}
}
