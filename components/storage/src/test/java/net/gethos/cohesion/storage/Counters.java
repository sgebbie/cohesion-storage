/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

public class Counters {
	public long data;
	
	public long store;
	public long search;
	public long fetch;
	public long delete;
	public long commit;
	
	private long mark;
	
	public Counters() {
		this.mark = System.nanoTime();
	}
	
	public long diff() {
		long n = System.nanoTime();
		long d = n - mark;
		mark = n;
		return d;
	}
	
	public long all() {
		return store + fetch + search + delete + commit;
	}
}