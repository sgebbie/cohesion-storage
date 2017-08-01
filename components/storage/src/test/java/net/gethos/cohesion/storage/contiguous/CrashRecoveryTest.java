/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.Map;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BTrees;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;
import net.gethos.cohesion.storage.RuntimeIOException;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.store.ByteBufferContiguousStore;
import net.gethos.cohesion.storage.store.ContiguousStore;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test crash recovery for the contiguous B-Tree.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class CrashRecoveryTest {

	// create a failing store delegate that fails after a certain amount of data writes,
	//   this can then be tuned to fail part way through the 1st or 2nd write phase of the commit

	private ContiguousStore underlying;

	@Before
	public void setUp() {
		this.underlying = new ByteBufferContiguousStore(10000);
		BTree bt = BTrees.newInstance(underlying,true,true);
		bt.close();
	}

	@After
	public void tearDown() {

	}

	@Test
	public void recoveryStateGood() {
		// create a new tree with content
		BTree bt = BTrees.newInstance(underlying, false, true);
		fillTreeAndClose(bt,0,100);
		// confirm that the store contains the necessary crash recovery data
		WinnowingIntegrity wi = new WinnowingIntegrity();
		Integrity.RecoveryState rs = wi.verify(underlying);
		assertEquals(Integrity.RecoveryState.VALID, rs);
		// check that the node checksums are all valid
		Map<Long,BufferRegion> invalid = wi.check(underlying);
		assertTrue(invalid.isEmpty());
	}

	@Test
	public void firstPhaseFailureRecovery() {
		// force a failure during commit, but before the first flush
		// confirm backing store represents the necessary crash state
		// open tree and confirm recovery to last-good-state
		phaseFailureRecovery(24500, Integrity.RecoveryState.INVALID_BACKUP);
	}


	@Test
	public void secondPhaseFailureRecovery() {
		// force a failure during commit, but after the first flush
		// confirm backing store represents the necessary crash state
		// open tree and confirm recovery to last-good-state
		phaseFailureRecovery(24700, Integrity.RecoveryState.INVALID_COMMIT);
	}

	private void checkDataIncluded(BTree bt, int start, int end) {
		ReadOnlyBTreeTransaction t = bt.openReadOnly();
		try {
			ByteBuffer b = ByteBuffer.allocate(100);
			for (int i = start; i < end; i++) {
				BTree.Key k = BTree.Key.key(0,i);
				b.clear();
				int l = t.fetch(k, 0, b);
				assertEquals(String.format("[%d] %d",i,l),100,l);
				b.flip();
				assertEquals(100,b.remaining());
				int x = b.getInt();
				assertEquals(i,x);
			}
			boolean ok = t.commit();
			assertTrue(ok);
		} finally {
			t.close();
		}
	}

	private void checkDataExcluded(BTree bt, int start, int end) {
		ReadOnlyBTreeTransaction t = bt.openReadOnly();
		try {
			for (int i = start; i < end; i++) {
				BTree.Key k = BTree.Key.key(0,i);
				BTree.Reference r = t.search(k);
				assertNull(r);
			}
			boolean ok = t.commit();
			assertTrue(ok);
		} finally {
			t.close();
		}
	}

	private void fillTreeAndClose(BTree bt, int start, int end) {
		try {
			BTreeTransaction t = bt.open();
			try {
				ByteBuffer b = ByteBuffer.allocate(100);
				for (int i = start; i < end; i++) {
					BTree.Key k = BTree.Key.key(0,i);
					b.clear();
					b.putInt(i);
					b.rewind();
					BTree.Reference r = t.store(k, 0, b);
					assertNotNull(r);
				}
				boolean ok = t.commit();
				assertTrue(ok);
			} finally {
				boolean ok = t.close();
				assertTrue(ok);
			}
		} finally {
			// close the tree normally
			bt.close();
		}
	}

	private void phaseFailureRecovery(long trip, Integrity.RecoveryState failureState) {

		WinnowingIntegrity wi = new WinnowingIntegrity();
		Integrity.RecoveryState rs;

		// create initial content
		BTree bt = BTrees.newInstance(underlying, false, true);
		//		System.out.println("...");
		fillTreeAndClose(bt,0,50);
		//		System.out.println("...@1");
		// verfiy that the first part is all valid
		rs = wi.verify(underlying);
		assertEquals(Integrity.RecoveryState.VALID, rs);
		Map<Long,BufferRegion> invalid = wi.check(underlying);
		assertTrue(invalid.isEmpty());
		// check that the tree contains earlier data and not later data
		bt = BTrees.newInstance(underlying, false, true);
		checkDataIncluded(bt,0,50);
		checkDataExcluded(bt,50,100);
		bt.close();

		// create a new tree with more content, but force a failure
		RegionIntegrityTest.FailableContiguousStore fallible = new RegionIntegrityTest.FailableContiguousStore(underlying);
		fallible.trigger(trip);
		try {
			bt = BTrees.newInstance(fallible, false, true);
			//			System.out.println("...");
			fillTreeAndClose(bt,51,100);
			fail("forced failure not triggered");
		} catch (RuntimeIOException e) {
			//			System.out.println("...@2");
		}

		// confirm that the store contains the necessary crash recovery data
		rs = wi.verify(underlying);
		assertEquals(failureState, rs);

		// open the tree again, this should trigger automatic recovery
		bt = BTrees.newInstance(underlying, false, true);
		bt.close();
		rs = wi.verify(underlying);
		assertEquals(Integrity.RecoveryState.VALID, rs);

		// check that the node checksums are all valid
		invalid = wi.check(underlying);
		assertTrue(invalid.isEmpty());

		// check that the tree contains earlier data and not later data
		bt = BTrees.newInstance(underlying, false, true);
		checkDataIncluded(bt,0,50);
		checkDataExcluded(bt,50,100);
		bt.close();
	}
}
