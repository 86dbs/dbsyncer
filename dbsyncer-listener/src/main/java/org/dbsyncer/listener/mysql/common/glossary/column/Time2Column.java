package org.dbsyncer.listener.mysql.common.glossary.column;

public final class Time2Column extends AbstractDatetimeColumn {
	private static final long serialVersionUID = 2408833111678694298L;

	private final java.sql.Time value;

	private Time2Column(java.sql.Time value) {
		this.value = value;
		this.timestampValue = new java.sql.Timestamp(value.getTime());
	}

	private Time2Column(java.sql.Timestamp value) {
		this.timestampValue = value;
		this.value = new java.sql.Time(value.getTime());
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}

	public java.sql.Time getValue() {
		return this.value;
	}

	public static final Time2Column valueOf(java.sql.Time value) {
		return new Time2Column(value);
	}

	public static final Time2Column valueOf(java.sql.Timestamp value) {
		return new Time2Column(value);
	}
}
