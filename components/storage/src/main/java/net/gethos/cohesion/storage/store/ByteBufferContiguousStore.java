/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.store;

import java.nio.ByteBuffer;


/**
 * A store for a tree that is held in a single
 * large ByteBuffer.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ByteBufferContiguousStore implements ContiguousStore {

	private ByteBuffer data;
	
	public ByteBufferContiguousStore(ByteBuffer data) {
		this.data = data;
	}
	
	public ByteBufferContiguousStore(int capacity) {
		this(ByteBuffer.allocate(capacity));
	}
	
	@Override
	public void close() {
		data = null;
	}

	@Override
	public long write(long position, ByteBuffer... buffers) {
		if (position < 0 || position > Integer.MAX_VALUE) throw new ArrayIndexOutOfBoundsException("The position: " + position + " is too large.");
		long l = 0;
		for (ByteBuffer b : buffers) {
			l += b.remaining();
		}
		final long required = position + l;
		if (required > data.capacity()) {
			long boundary = required;
			boundary += required >> 1;
			boundary >>= 4;
			boundary <<= 4;
			truncate(boundary);
		}
		data.clear();
		data.position((int)position);
		for (ByteBuffer b : buffers) {
			data.put(b);
		}
		return l;
	}

	@Override
	public long read(long position, ByteBuffer... buffers) {
		if (position < 0 || position > Integer.MAX_VALUE) throw new ArrayIndexOutOfBoundsException("The position: " + position + " is too large.");
		long l = 0;
		int p = (int)position;
		data.clear();
		data.position(p);
		for (ByteBuffer b : buffers) {
			int r = b.remaining();
			l += r;
			p += r;
			data.limit(p);
			b.put(data);
		}
		return l;
	}

	@Override
	public long truncate(long length) {
		if (length < 0 || length > Integer.MAX_VALUE) throw new IllegalArgumentException("The length: " + length + " is larger than the maximum memory backed tree can be: " + Integer.MAX_VALUE);
		int len = (int)length;
		data.clear();
		if (data.capacity() != len) {
			ByteBuffer t = ByteBuffer.allocate(len);
			if (data.remaining() > len) data.limit(len);
			t.put(data);
			data = t;
		}
		return data.capacity();
	}

	@Override
	public long size() {
		return data.capacity();
	}

	@Override
	public void force() {
	}
	
}
