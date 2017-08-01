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
 * Defines how the integrity (across crashes) is maintained
 * in order to support recovery and remain in a consistent state.
 * <p>
 * <code>
 * <pre>
 * // perform actions
 * integrity.backup(store,cache,modified);
 * // perform write
 * integrity.commit(store);
 * </pre>
 * </code>
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface Integrity<T> {

	/**
	 * The state of the recovery data.
	 */
	public enum RecoveryState {
		/**
		 * Indicates that the recovery region is invalid.
		 * <p>
		 * The last commit is assumed valid due to double flush,
		 * but a full check contents should be performed (e.g. using node checksums).
		 */
		INVALID_BACKUP,
		/**
		 * The recovery region is valid, but based on this the last commit data is invalid.
		 * <p>
		 * The recovery region can be used to restore the last-good-state.
		 */
		INVALID_COMMIT,
		/**
		 * Both the recovery region and the tree are valid.
		 */
		VALID
	}

	/**
	 * Performs the first phase of the recovery checkpoint.
	 * <p>
	 * Write out a backup copy of regions about to be modified with new node data.
	 * <p>
	 * Once written the store is synchronously force flushed to underlying storage.
	 * 
	 * @param store
	 * @param cache - used to fetch original node data if possible
	 * @param modified - list of modified regions together with their destination offsets,
	 *                        for which a backup should be made.
	 */
	public void backup(ContiguousStore store, RegionCache<T> cache, Map<Long, T> modified);

	/**
	 * Performs the second phase of the recovery checkpoint.
	 * <p>
	 * Performs a synchronous force flush to underlying storage.
	 * <p>
	 * This expects any other updates to have been performed following the <code>backup()</code> call.
	 * It essentially just formalises the need for the second flush.
	 * 
	 * @param store
	 */
	public void commit(ContiguousStore store);

	/**
	 * This analyses the store and decides what type of recovery will need
	 * to be performed - if any.
	 * <p>
	 * No data is altered.
	 * 
	 * @param store
	 * @return the current state indicating what type of recovery is needed
	 */
	public Integrity.RecoveryState verify(ContiguousStore store);

	/**
	 * Analyse the store an perform any necessary recovery, if required.
	 * <p>
	 * If a recovery is required, this is carried out and flushed to disk. That is, if:
	 * <ul>
	 *   <li>both the commit region and the backup regions are valid, then no action is performed.</li>
	 *   <li>if the backup region is invalid, then it is rebuilt from the valid commit data.</li>
	 *   <li>if the commit region is invalid, then it is rebuilt from the valid backup data.</li>
	 * </ul>
	 * <p>
	 * If the store contents where modified, this will additionally perform the necessary synchronous
	 * forced flushes.
	 * <p>
	 * Following this action a subsequent call to <code>verify(store)</code> should always return
	 * {@link RecoveryState#VALID}.
	 * 
	 * @param store
	 * @return the recover state prior to recovery
	 *         (i.e. the same state that would have been returned by a
	 *          call to {@link Integrity#verify(ContiguousStore)})
	 */
	public Integrity.RecoveryState restore(ContiguousStore store);

	/**
	 * Walks the contents and verifies that every regions' checksum is correct.
	 * 
	 * @param store
	 * @return an empty map if all regions are valid, otherwise the offset and copies of invalid regions.
	 */
	public Map<Long, T> check(ContiguousStore store);
}