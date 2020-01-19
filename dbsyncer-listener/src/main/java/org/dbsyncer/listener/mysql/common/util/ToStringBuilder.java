package org.dbsyncer.listener.mysql.common.util;

public final class ToStringBuilder {
	private int count;
	private final StringBuilder builder;

	public ToStringBuilder(Object object) {
		String name = ClassUtils.getShortClassName(object.getClass().getName());
		this.builder = new StringBuilder(32); this.builder.append(name).append("[");
	}

	@Override
	public String toString() {
		return this.builder.append("]").toString();
	}

	public ToStringBuilder append(String key, int value) {
		if(count++ > 0) this.builder.append(',');
		this.builder.append(key).append('=').append(value);
		return this;
	}

	public ToStringBuilder append(String key, long value) {
		if(count++ > 0) this.builder.append(',');
		this.builder.append(key).append('=').append(value);
		return this;
	}

	public ToStringBuilder append(String key, byte value) {
		if(count++ > 0) this.builder.append(',');
		this.builder.append(key).append('=').append(value);
		return this;
	}

	public ToStringBuilder append(String key, String value) {
		if(count++ > 0) this.builder.append(',');
		this.builder.append(key).append('=').append(value == null ? "<null>" : value);
		return this;
	}

	public ToStringBuilder append(String key, Object value) {
		if(count++ > 0) this.builder.append(',');
		this.builder.append(key).append('=').append(value == null ? "<null>" : value);
		return this;
	}
}
