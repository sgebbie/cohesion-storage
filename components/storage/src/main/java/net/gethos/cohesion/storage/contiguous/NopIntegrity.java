/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.Map;

import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class NopIntegrity<T> implements Integrity<T> {

	@Override
	public void backup(ContiguousStore store, RegionCache<T> cache, Map<Long, T> modified) {
		
	}

	@Override
	public void commit(ContiguousStore store) {
		
	}

	@Override
	public Integrity.RecoveryState verify(ContiguousStore store) {
		return null;
	}

	@Override
	public Integrity.RecoveryState restore(ContiguousStore store) {
		return null;
	}

	@Override
	public Map<Long, T> check(ContiguousStore store) {
		return null;
	}

}
