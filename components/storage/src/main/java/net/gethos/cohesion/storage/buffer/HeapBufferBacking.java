/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.io.PrintStream;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBufferBacking implements BTreeBacking {

	private final HeapBufferStorage storage;
	private final NodeCapacities nodeCapacities;
	
	public HeapBufferBacking(int capacity) {
		this.nodeCapacities = new ByteBufferNodeCapacities(capacity);
		this.storage = new HeapBufferStorage(nodeCapacities);
	}
	
	@Override
	public void close() {
		
	}
	
	@Override
	public BTreeBackingTransaction open() {
		return new HeapBufferBackingTransaction(storage, nodeCapacities);
	}
	
	@Override
	public ReadOnlyBTreeBackingTransaction openReadOnly() {
		return new HeapBufferBackingTransaction(storage, nodeCapacities);
	}

	/**
	 * @param out
	 */
	public void dump(PrintStream out) {
		storage.dump(out);
	}

}
