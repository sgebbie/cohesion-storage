/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BackedBTree implements BTree {

	private final BTreeBacking backing;
	
	public BackedBTree(BTreeBacking backing) {
		this.backing = backing;
	}
	
	@Override
	public void close() {
		backing.close();
	}
	
	@Override
	public ReadOnlyBTreeTransaction openReadOnly() {
		ReadOnlyBTreeBackingTransaction t = backing.openReadOnly();
		return new ReadOnlyTransactionBTree(t);
	}

	@Override
	public BTreeTransaction open() {
		BTreeBackingTransaction t = backing.open();
		return new TransactionBTree(t);
	}

	public BTreeBacking backing() {
		return backing;
	}

	@Override
	public int depth() {
		ReadOnlyBTreeBackingTransaction t = null;
		try {
			t = backing.openReadOnly();
			return t.depth();
		} finally {
			if (t != null) t.close();
		}
	}

}
