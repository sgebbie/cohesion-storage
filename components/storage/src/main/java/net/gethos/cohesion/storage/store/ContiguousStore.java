/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.store;

import java.nio.ByteBuffer;

/**
 * Provides access to data stored in a single
 * contiguous region.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface ContiguousStore {

	/**
	 * Write data to the store.
	 * <p>
	 * The store is automatically grown to accommodate at least the highest byte written.
	 * 
	 * @param position - offset to which to start writing
	 * @param buffers - buffers to drain
	 * @return number of bytes written
	 */
	public long write(long position, ByteBuffer... buffers);

	/**
	 * Read data from the store.
	 * 
	 * @param position - offset from which to start reading
	 * @param buffers - buffers to fill
	 * @return number of bytes read
	 */
	public long read(long position, ByteBuffer... buffers);
	
	/**
	 * Truncate (grow or shrink) the store to the closest
	 * boundary size, that is larger or equal to the given size.
	 * 
	 * @param length
	 * @return the new size.
	 */
	public long truncate(long length);
	
	/**
	 * Query the size of the underlying storage.
	 * 
	 * @return underlying storage size in bytes.
	 */
	public long size();
	
	/**
	 * Force flush data to disk i.e. <code>fsync()</code>
	 */
	public void force();
	
	/**
	 * Close the store.
	 * <p>
	 * Note, no implicit flush is performed.
	 */
	public void close();
}
