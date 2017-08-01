/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.gethos.cohesion.common.HexBuilder;
import net.gethos.cohesion.storage.RuntimeIOException;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.cache.TrivialCache;
import net.gethos.cohesion.storage.store.ByteBufferContiguousStore;
import net.gethos.cohesion.storage.store.ContiguousStore;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 * 
 */
public class RegionIntegrityTest {

	private final static int TEST_COUNT_REGION = 10;
	private final static int TEST_MIN_REGION = 100;
	private final static int TEST_MAX_REGION = 200;
	private final static int TEST_MIN_GAP = 0;
	private final static int TEST_MAX_GAP = 200;

	private RegionIntegrity<ByteBuffer> integrity;
	private FailableContiguousStore store;
	private RegionCache<ByteBuffer> cache;
	private MockAllocationAccessor allocation;

	private Random rand;

	private Map<Long, ByteBuffer> initial;
	private Map<Long, ByteBuffer> modified;

	@Before
	public void setUp() {
		this.rand = new Random(123);
		this.cache = new TrivialCache<ByteBuffer>();
		ByteBufferContiguousStore internalStore = new ByteBufferContiguousStore(10000);
		this.store = new FailableContiguousStore(internalStore);
		this.allocation = new MockAllocationAccessor();
		this.integrity = new RegionIntegrity<ByteBuffer>(new ByteBufferByteBufferAccessor(),allocation);

		this.initial = createRegions();
		this.modified = createRegions();
	}

	private static class ByteBufferByteBufferAccessor implements RegionIntegrity.ByteBufferAccessor<ByteBuffer> {
		@Override
		public ByteBuffer buffer(ByteBuffer t) { return t; }
	}

	private static class MockAllocationAccessor implements RegionIntegrity.AllocationAccessor {

		public long tail;

		@Override
		public long tail() {
			return tail;
		}
	}

	private long tail(Map<Long, ByteBuffer> regions) {
		long tail = 0;
		for (Map.Entry<Long,ByteBuffer> e : regions.entrySet()) {
			long etail = e.getKey() + e.getValue().capacity();
			if (etail > tail) tail = etail;
		}
		return tail;
	}

	private void performUpdate(Map<Long, ByteBuffer> initial, Map<Long, ByteBuffer> modified, long failTrigger) {
		try {
			// create an initial set of non overlapping regions
			writeRegions(initial);
			long itail = tail(initial);

			// create the modified set
			long mtail = tail(modified);

			store.goodRemaining = failTrigger;
			store.goodCount = 0;

			// set the mock allocation tail
			allocation.tail = Math.max(itail, mtail);
			// create backup and commit
			integrity.backup(store, cache, modified);
			writeRegions(modified);
			integrity.commit(store);
		} finally {
			store.goodRemaining = Long.MAX_VALUE;
		}
	}

	@Test
	public void backup() {

		performUpdate(initial, modified, Long.MAX_VALUE);

		// load the backup metadata
		RegionIntegrity<ByteBuffer>.Backup backup = integrity.new Backup();
		backup.load(store);
		assertNotNull(backup.offsets);
		assertNotNull(backup.lengths);
		assertEquals(backup.count, backup.offsets.length);
		assertEquals(backup.count, backup.lengths.length);
		Map<Long,Integer> recordLengths = new HashMap<Long, Integer>();
		for (int i = 0; i < backup.count; i++) {
			recordLengths.put(backup.offsets[i],backup.lengths[i]);
		}
		Set<Long> recordedOffsets = new HashSet<Long>(recordLengths.keySet());

		//   check the number of regions
		assertEquals(modified.size(), backup.count);
		//   check the offsets
		Set<Long> modifiedOffsets = new HashSet<Long>(modified.keySet());
		assertEquals(modifiedOffsets, recordedOffsets);
		//   check lengths
		for (Map.Entry<Long, ByteBuffer> e : modified.entrySet()) {
			assertEquals((int)e.getValue().capacity(), (int)recordLengths.get(e.getKey()));
		}

		// now re-calculate the hashes
		byte[] calculatedModifiedHash = backup.calculateModifiedHash(store);
		byte[] calculatedRecoveryHash = backup.calculateRecoveryHash(store);
		String mcmp = String.format("%s ?= %s", HexBuilder.toHex(backup.modifiedHash), HexBuilder.toHex(calculatedModifiedHash));
		String rcmp = String.format("%s ?= %s", HexBuilder.toHex(backup.recoveryHash), HexBuilder.toHex(calculatedRecoveryHash));
		//		System.out.printf("mcmp: %s%nrcmp: %s%n",mcmp,rcmp);
		assertTrue(mcmp,Arrays.equals(backup.modifiedHash, calculatedModifiedHash));
		assertTrue(rcmp,Arrays.equals(backup.recoveryHash, calculatedRecoveryHash));
	}

	@Test
	public void verifyGood() {
		performUpdate(initial, modified, Long.MAX_VALUE);
		Integrity.RecoveryState recoveryState = integrity.verify(store);
		assertEquals(Integrity.RecoveryState.VALID, recoveryState);
	}

	@Test
	public void restoreGood() {
		performUpdate(initial, modified, Long.MAX_VALUE);
		Integrity.RecoveryState recoveryState = integrity.restore(store);
		assertEquals(Integrity.RecoveryState.VALID, recoveryState);
		verifyContents(modified);
	}

	@Test
	public void verifyBadBackup() {
		try {
			// set the trigger to force a failure during the call to integrity.backup(...)
			performUpdate(initial, modified, 1600);
			fail("should trigger I/O exception");
		} catch (RuntimeIOException e) {
		}
		Integrity.RecoveryState recoveryState = integrity.verify(store);
		assertEquals(Integrity.RecoveryState.INVALID_BACKUP, recoveryState);
	}

	@Test
	public void restoreBadBackup() {
		try {
			// set the trigger to force a failure during the call to integrity.backup(...)
			performUpdate(initial, modified, 1600);
			fail("should trigger I/O exception");
		} catch (RuntimeIOException e) {
		}
		Integrity.RecoveryState recoveryState = integrity.restore(store);
		assertEquals(Integrity.RecoveryState.INVALID_BACKUP, recoveryState);
		verifyContents(initial);
	}

	@Test
	public void verifyBadCommit() {
		try {
			// set the trigger to force a failure after the call to integrity.backup(...) and during the write of the modified data
			performUpdate(initial, modified, 3000);
			fail("should trigger I/O exception");
		} catch (RuntimeIOException e) {
		}
		Integrity.RecoveryState recoveryState = integrity.verify(store);
		assertEquals(Integrity.RecoveryState.INVALID_COMMIT, recoveryState);
	}

	@Test
	public void restoreBadCommit() {
		try {
			// set the trigger to force a failure after the call to integrity.backup(...) and during the write of the modified data
			performUpdate(initial, modified, 3000);
			fail("should trigger I/O exception");
		} catch (RuntimeIOException e) {
		}
		Integrity.RecoveryState recoveryState = integrity.restore(store);
		assertEquals(Integrity.RecoveryState.INVALID_COMMIT, recoveryState);
		verifyContents(initial);
	}

	private void verifyContents(Map<Long, ByteBuffer> expect) {
		ByteBuffer scratch = null;
		for (Map.Entry<Long, ByteBuffer> e : expect.entrySet()) {
			long pos = e.getKey();
			ByteBuffer x = e.getValue();
			// load the data from the store
			if (scratch == null || scratch.capacity() < x.capacity()) scratch = ByteBuffer.allocate(x.capacity());
			scratch.clear();
			scratch.limit(x.capacity());
			while (scratch.hasRemaining()) pos += store.read(pos, scratch);
			scratch.flip();

			// now compare the expected value
			for (int p = 0; p < x.capacity(); p++) {
				assertEquals(String.format("offset=%d p=%d",e.getKey(),p), x.get(p), scratch.get(p));
			}
		}
	}

	private Map<Long, ByteBuffer> createRegions() {
		Map<Long, ByteBuffer> regions = new HashMap<Long, ByteBuffer>();
		long pos = rand(TEST_MIN_GAP, TEST_MAX_GAP);
		for(int i = 0; i < TEST_COUNT_REGION; i++) {
			int len = rand(TEST_MIN_REGION, TEST_MAX_REGION);
			int gap = rand(TEST_MIN_GAP, TEST_MAX_GAP);
			byte[] data = new byte[len];
			Arrays.fill(data, (byte)i);
			ByteBuffer b = ByteBuffer.wrap(data);
			regions.put(pos, b);
			pos += len + gap;
		}
		return regions;
	}

	private int rand(int low, int high) {
		assert(low < high);
		return low + rand.nextInt(1 + high - low);
	}

	private long writeRegions(Map<Long, ByteBuffer> modified) {
		long tail = 0;
		for(Map.Entry<Long, ByteBuffer> x : modified.entrySet()) {
			long offset = x.getKey();
			assert(offset>=0);
			ByteBuffer b = x.getValue();
			b.rewind();
			while (b.hasRemaining()) offset += store.write(offset,b);
			if (offset > tail) tail = offset;
		}
		return tail;
	}

	static class FailableContiguousStore implements ContiguousStore {

		private final ContiguousStore delegate;

		public long goodRemaining;
		public long goodCount;

		public FailableContiguousStore(ContiguousStore delegate) {
			this.delegate = delegate;
			this.goodRemaining = Long.MAX_VALUE;
			this.goodCount = 0;
		}

		public void trigger(long remaining) {
			this.goodRemaining = remaining;
			this.goodCount = 0;
		}

		@Override
		public long write(long position, ByteBuffer... buffers) {

			int[] oldPositions = new int[buffers.length];
			int[] oldLimits = new int[buffers.length];

			long r = goodRemaining;
			// limit buffers to simulate failed write
			for (int i = 0; i < buffers.length; i++) {
				oldPositions[i] = buffers[i].position();
				oldLimits[i] = buffers[i].limit();
				if (buffers[i].remaining() > r) {
					buffers[i].limit(buffers[i].position()+(int)r);
				}
				r -= buffers[i].remaining();
			}

			// perform write
			long l = delegate.write(position, buffers);

			// restore old limits
			for (int i = 0; i < buffers.length; i++) {
				buffers[i].limit(oldLimits[i]);
			}

			// update remaining counters
			goodCount += l;
			if (goodRemaining != Long.MAX_VALUE) goodRemaining -= l;
			if (goodRemaining <= 0) {
				for (int i = 0; i < buffers.length; i++) {
					if (buffers[i].hasRemaining()) throw new RuntimeIOException("No more good bytes");
				}
			}
			return l;
		}

		@Override
		public long read(long position, ByteBuffer... buffers) {
			return delegate.read(position, buffers);
		}

		@Override
		public long truncate(long length) {
			return delegate.truncate(length);
		}

		@Override
		public long size() {
			return delegate.size();
		}

		@Override
		public void force() {
			delegate.force();
		}

		@Override
		public void close() {
			delegate.close();
		}

	}
}
