/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.backing.BTreeLeafNode;


/**
 * Metadata for referencing data to be stored outside of a node item.
 * <p>
 * This metadata is stored within the node item data.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
class ExtranodeReference {
	
	public static final int SIZE = 2*StorageConstants.SIZEOF_LONG;

	public long offset;
	public long size;
	
	public ExtranodeReference() {
		this.offset = 0;
		this.size = 0;
	}
	
	@Override
	public String toString() {
		return String.format("@%d[%d]", offset, size);
	}
	
	public static ExtranodeReference read(int idx, BTreeLeafNode nl) {
		ExtranodeReference xr = new ExtranodeReference();
		xr.readFrom(idx, nl);
		return xr;
	}
	
	private void readFrom(ByteBuffer data) {
		offset = data.getLong();
		size = data.getLong();
	}
	
	private void writeTo(ByteBuffer data) {
		data.putLong(offset);
		data.putLong(size);
	}
	
	public boolean readFrom(int idx, BTreeLeafNode nl) {
		ByteBuffer bb = ByteBuffer.allocate(StorageConstants.SIZEOF_LONG*2);
		bb.order(StorageConstants.NETWORK_ORDER);
		int r = nl.read(idx, 0, bb);
		if (r != bb.capacity()) return false;
		bb.flip();
		readFrom(bb);
		return true;
	}
	
	public boolean writeTo(int idx, BTreeLeafNode nl) {
		ByteBuffer bb = ByteBuffer.allocate(StorageConstants.SIZEOF_LONG*2);
		bb.order(StorageConstants.NETWORK_ORDER);
		writeTo(bb);
		bb.flip();
		int r = nl.write(idx, 0, bb);
		return r == bb.capacity() ? true : false;
	}

	public static ExtranodeReference create(long offset, long size) {
		ExtranodeReference xr = new ExtranodeReference();
		xr.offset = offset;
		xr.size = size;
		return xr;
	}
}
