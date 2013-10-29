package org.wood.hbase.examples;

import org.joda.time.DateTime;

public abstract class Twit {
	public String user;
	public String twit;
	public DateTime dt;

	@Override
	public String toString() {
		return String.format("<User %s at %s: %s>", user, dt, twit);
	}
}