/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.common.HexBuilder;
import net.gethos.cohesion.common.Sha1Utils;
import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.RuntimeIOException;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.cache.TrivialCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * This implements the integrity process for persistence
 * of regions in a contiguous store.
 * <p>
 * The resultant file layout is:
 * <pre>
| S | U U ... M_1 ... M_2 ... M_n ...| ...F... | R_S R_1 ... R_n | O_S O_1 ... O_n | L_S L_1 ... L_n | hash(S+M_i) | hash(recovery data) | n | EOF.

where:
  S = super block
  U = unmodified region (node or raw)
  M_i = modified node
  F = unused (free) space
  R_S = super block recovery copy
  O_S = offset of header (generally zero)
  L_S = length of super block
  R_i = modified node recovery copies
  O_i = offset of modified node i
  L_i = length of modified node i
  hash = strong integrity hash
  n = number of nodes modified
  EOF = end-of-file
and:
  n, L_x are 32-bit integers
  O_x are 64-bit integers
  hash is 20-byte SHA1
 * </pre>
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class RegionIntegrity<R> implements Integrity<R> {

	public interface ByteBufferAccessor<T> {
		/**
		 * Access the encapsulated ByteBuffer.
		 * 
		 * @param t
		 * @return return the ByteBuffer encapsulated in <code>t</code>
		 */
		public ByteBuffer buffer(T t);
	}

	public interface AllocationAccessor {
		/**
		 * Find the tail of the used storage.
		 * 
		 * @return the offset of the first byte beyond the highest used byte of storage
		 */
		public long tail();
	}

	private static final int DEFAULT_SCRATCH_SIZE = 4096;

	private static final byte[] ZERO_HASH = new byte[StorageConstants.SIZEOF_SHA1];

	private final ByteBufferAccessor<R> accessor;
	private final AllocationAccessor allocation;
	private final MessageDigest sha1;

	public RegionIntegrity(ByteBufferAccessor<R> accessor) {
		this(accessor,null);
	}

	public RegionIntegrity(ByteBufferAccessor<R> accessor, AllocationAccessor allocation) {
		this.accessor = accessor;
		this.allocation = allocation;
		this.sha1 = sha1();
	}

	@Override
	public void backup(ContiguousStore store, RegionCache<R> cache, Map<Long, R> modified) {
		if (allocation == null) throw new IllegalStateException("Backups can not be performed if no allocation information is provided.");
		backup(store,cache,modified,allocation.tail());
	}

	private void backup(ContiguousStore store, RegionCache<R> cache, Map<Long, R> modified, final long tail) {
		assert(tail >= 0);
		try {
			// set up metadata
			int count = modified.size();

			ByteBuffer metadata = ByteBuffer.allocate(sizeOfMetadata(count));
			metadata.order(StorageConstants.NETWORK_ORDER);
			int rlen = metadata.capacity();
			// dump
			//			for (Map.Entry<Long, R> e : modified.entrySet()) {
			//				ByteBuffer b = accessor.buffer(e.getValue());
			//				System.out.printf("> [%6d:%6d] %s%n", e.getKey(), b.capacity(), HexBuilder.toHex(b.array()));
			//			}

			// collect offsets
			for (long o : modified.keySet()) {
				metadata.putLong(o);
			}

			// (note, while it would be tempting to write out the modified data at
			//  the same time as calculating the hash, that would break the
			//  semantics of the integrity process. Rather the modified data must
			//  only be written after the backup is synced to disk. For this
			//  reason it is also safe to do this before we make a backup copy)

			// collect lengths and update modified hash
			sha1.reset();
			for (R r : modified.values()) {
				ByteBuffer b = accessor.buffer(r);
				// collect lengths
				int len = b.capacity();
				metadata.putInt(len);
				rlen += len;
				// update hash
				b.rewind();
				sha1.update(b);
			}
			byte[] modifiedHash = sha1.digest();

			// set up recovery location at the end of the store
			//			assert(count == modified.size());
			//			final long tail = allocation.tail();
			//			assert(count == modified.size());

			long end = store.size();
			// note, we could have tail > end at this point due to growth in the tree that is not yet committed
			// check if we need to grow the store
			if (tail + rlen > end) {
				end = store.truncate(tail+rlen);
			}

			assert (end - tail >= rlen) : String.format("tail = %d end = %d rlen = %d", tail, end, rlen);
			final long start = end - rlen;
			long rpos = start;

			// record backup copy of regions and update recovery hash
			sha1.reset();
			ByteBuffer scratch = null;
			assert(count == modified.size());
			for (Map.Entry<Long, R> e : modified.entrySet()) {
				final long offset = e.getKey();
				final int len = accessor.buffer(e.getValue()).capacity();

				ByteBuffer backupSource = null;

				// check if the region to be modified is currently in the cache
				R cached = cache.get(offset);
				if (cached != null) {
					backupSource = accessor.buffer(cached);
					if (backupSource.capacity() != len) backupSource = null;
					else backupSource.rewind();
					assert(backupSource == null || backupSource.remaining() == len) : String.format("len = %d remaining = %d",len,backupSource.remaining());
				}
				// if not cached, then fetch from store
				if (backupSource == null) {
					if (scratch == null || scratch.capacity() != len) scratch = ByteBuffer.allocate(len);
					scratch.clear();
					while(scratch.hasRemaining()) {
						long l = store.read(offset+scratch.position(), scratch);
						if (l < 0) throw new RuntimeIOException(String.format("failed to read to %d",offset));
					}
					scratch.flip();
					backupSource = scratch;
				}
				// update the recovery hash
				sha1.update(backupSource);
				backupSource.rewind();

				//				System.out.printf("= [%6d:%6d] (%d) %s%n", offset, len, rpos, HexBuilder.toHex(backupSource.array()));

				// now write out the copy
				while (backupSource.hasRemaining()) {
					long l = store.write(rpos, backupSource);
					if (l < 0) throw new RuntimeIOException(String.format("failed to write to %d",rpos));
					rpos += l;
				}

			}

			//			assert (rpos == end - metadata.capacity()) : String.format("count = %d start = %d end = %d rpos = %d rlen = %d metadata.capacity = %d", count, start, end, rpos, rlen, metadata.capacity());

			// include all the meta data into the recovery hash
			metadata.put(modifiedHash);
			metadata.put(ZERO_HASH); // place holder
			metadata.putInt(count);
			metadata.flip();
			sha1.update(metadata);

			// complete calculation of the recovery hash
			byte[] recoveryHash = sha1.digest();
			metadata.position(metadata.capacity() - StorageConstants.SIZEOF_INT - StorageConstants.SIZEOF_SHA1);
			metadata.put(recoveryHash);
			metadata.rewind();

			//System.out.printf("> modified hash = %s%n", HexBuilder.toHex(modifiedHash));
			//System.out.printf("> recovery hash = %s%n", HexBuilder.toHex(recoveryHash));
			//System.out.printf("> modified count = %d%n", count);
			//System.out.printf("> start=%d end=%d tail=%d rlen=%d %n",start,end,tail,rlen);

			// finally record the metadata
			while (metadata.hasRemaining()) {
				long l = store.write(rpos, metadata);
				if (l < 0) throw new RuntimeIOException(String.format("failed to write to %d",rpos));
				rpos += l;
			}

			assert (rpos == end) : String.format("count = %d start = %d end = %d rpos = %d rlen = %d metadata.capacity = %d", count, start, end, rpos, rlen, metadata.capacity());

		} finally {
			store.force();
		}
	}

	@Override
	public void commit(ContiguousStore store) {
		store.force();
	}

	@Override
	public Integrity.RecoveryState verify(ContiguousStore store) {
		Backup b = new Backup();
		return verify(store, b);
	}

	@Override
	public Integrity.RecoveryState restore(ContiguousStore store) {
		Backup b = new Backup();
		Integrity.RecoveryState r = verify(store,b);
		if (r == Integrity.RecoveryState.INVALID_COMMIT) {
			// restore from backup data
			recover(store,b);
		}

		if (r != Integrity.RecoveryState.VALID) {
			if (b.start >= 0 && b.end >= 0 && b.count >= 0) {
				// reset to a good recovery state
				backup(store,new TrivialCache<R>(),new HashMap<Long, R>(), b.start);
			}
		}

		return r;
	}

	private void recover(ContiguousStore store, Backup b) {
		try {
			long rpos = b.start;
			ByteBuffer scratch = null;
			for (int i = 0; i < b.count; i++) {
				// create space for the backup
				if (scratch == null || scratch.capacity() < b.lengths[i]) scratch = ByteBuffer.allocate(b.lengths[i]);
				scratch.clear();
				scratch.limit(b.lengths[i]);
				// read the backup data
				while (scratch.hasRemaining()) rpos += store.read(rpos, scratch);
				scratch.flip();

				// write the backup data back to its original position
				long pos = b.offsets[i];
				while (scratch.hasRemaining()) pos += store.write(pos, scratch);
			}
		} finally {
			store.force();
		}
	}

	private Integrity.RecoveryState verify(ContiguousStore store, Backup b) {
		b.load(store);
		//b.dump();

		if (b.count < 0 || b.start < 0 || b.end < 0) return Integrity.RecoveryState.INVALID_BACKUP;
		if (b.recoveryHash == null || b.modifiedHash == null || b.lengths == null || b.offsets == null) return Integrity.RecoveryState.INVALID_BACKUP;
		for (int l : b.lengths) if (l < 0) return Integrity.RecoveryState.INVALID_BACKUP;
		for (long o : b.lengths) if (o < 0) return Integrity.RecoveryState.INVALID_BACKUP;

		byte[] calculatedRecoveryHash = b.calculateRecoveryHash(store);
		//System.out.printf("& calculated recovery hash = %s%n", HexBuilder.toHex(calculatedRecoveryHash));
		if (!Arrays.equals(b.recoveryHash, calculatedRecoveryHash)) return Integrity.RecoveryState.INVALID_BACKUP;

		byte[] calculatedModifiedHash = b.calculateModifiedHash(store);
		//System.out.printf("& calculated modified hash = %s%n", HexBuilder.toHex(calculatedModifiedHash));
		if (!Arrays.equals(b.modifiedHash, calculatedModifiedHash)) return Integrity.RecoveryState.INVALID_COMMIT;

		return Integrity.RecoveryState.VALID;
	}

	@Override
	public Map<Long, R> check(ContiguousStore store) {
		return new HashMap<Long,R>();
	}

	private static MessageDigest sha1() {
		try {
			return MessageDigest.getInstance(Sha1Utils.SHA1_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeIOException("Unable to acquire digester",e);
		}
	}

	private static int sizeOfMetadata(int count) {
		return
		+ StorageConstants.SIZEOF_INT
		+ 2 * StorageConstants.SIZEOF_SHA1
		+ count*(StorageConstants.SIZEOF_INT + StorageConstants.SIZEOF_LONG)
		;
	}

	class Backup {

		/**
		 * First byte of the backup region
		 */
		public long start;

		/**
		 * First byte after the end of the backup region (i.e. EOF)
		 */
		public long end;

		/**
		 * Number of regions tracked by the backup
		 */
		public int count;

		/**
		 * Hash (SHA1), as recorded in the backup region, of recovery data stored in backup region.
		 */
		public byte[] recoveryHash;
		/**
		 * Hash (SHA1), as recorded in the backup region, of the modified data to be stored after the backup is made.
		 */
		public byte[] modifiedHash;

		/**
		 * Length of each region that was modified.
		 */
		public int[] lengths;

		/**
		 * Offset of each region that was modified.
		 */
		public long[] offsets;

		public void dump() {

			assert(count == lengths.length && count == offsets.length);

			for (int i = 0; i < offsets.length; i++) {
				System.out.printf("# [%6d:%6d] %s%n", offsets[i], lengths[i], "");
			}

			System.out.printf("# modified hash = %s%n", HexBuilder.toHex(modifiedHash));
			System.out.printf("# recovery hash = %s%n", HexBuilder.toHex(recoveryHash));
			System.out.printf("# modified count = %d%n", count);
			System.out.printf("# start=%d end=%d%n",start,end);
		}

		/**
		 * Loads the recovery data, and populates the fields.
		 * 
		 * @param store
		 */
		public void load(ContiguousStore store) {

			// reset
			this.start = -1;
			this.end = -1;
			this.count = -1;
			this.recoveryHash = null;
			this.modifiedHash = null;
			this.offsets = null;
			this.lengths = null;

			// inspect the underlying data in the store
			long eof = store.size();
			this.end = eof;
			if (eof < StorageConstants.SIZEOF_INT) return;

			// read the count
			ByteBuffer bint = ByteBuffer.allocate(StorageConstants.SIZEOF_INT);
			bint.order(StorageConstants.NETWORK_ORDER);
			while (bint.hasRemaining()) store.read(eof - bint.capacity() + bint.position(), bint);
			bint.flip();
			this.count = bint.getInt();
			if (this.count < 0) return;

			// read in the metadata
			ByteBuffer metadata = ByteBuffer.allocate(sizeOfMetadata(this.count));
			metadata.order(StorageConstants.NETWORK_ORDER);
			long pos = eof - metadata.capacity();
			if (pos < 0) return;
			while (metadata.hasRemaining()) pos += store.read(pos, metadata);
			metadata.flip();
			this.offsets = new long[this.count];
			this.lengths = new int[this.count];
			this.modifiedHash = new byte[StorageConstants.SIZEOF_SHA1];
			this.recoveryHash = new byte[StorageConstants.SIZEOF_SHA1];
			long rlen = metadata.capacity();
			for (int i = 0; i < this.count; i++) this.offsets[i] = metadata.getLong();
			for (int i = 0; i < this.count; i++) { this.lengths[i] = metadata.getInt(); rlen += this.lengths[i]; }
			metadata.get(this.modifiedHash);
			metadata.get(this.recoveryHash);

			this.start = eof - rlen;
		}

		/**
		 * Calculates the hash directly from the modified regions in the store.
		 * This is used during verification.
		 * 
		 * @param store
		 * @return hash of modified regions
		 */
		public byte[] calculateModifiedHash(ContiguousStore store) {
			sha1.reset();
			assert(offsets != null);
			assert(lengths != null);
			assert(lengths.length == count && offsets.length == count);

			ByteBuffer scratch = null;
			for (int i = 0; i < count; i++) {
				if (scratch == null || scratch.capacity() != lengths[i]) scratch = ByteBuffer.allocate(lengths[i]);
				scratch.clear();
				while(scratch.hasRemaining()) store.read(offsets[i] + scratch.position(), scratch);
				scratch.flip();
				//System.out.printf("& [%6d:%6d] %s%n", offsets[i], lengths[i], HexBuilder.toHex(scratch.array()));
				sha1.update(scratch);
			}

			return sha1.digest();
		}

		/**
		 * Calculates the hash directly from the recovery regions in the store.
		 * This is used during verification.
		 * 
		 * @param store
		 * @return hash of recovery regions.
		 */
		public byte[] calculateRecoveryHash(ContiguousStore store) {
			sha1.reset();
			ByteBuffer scratch = ByteBuffer.allocate(DEFAULT_SCRATCH_SIZE);
			long pos = start;
			long rend = end - StorageConstants.SIZEOF_INT - StorageConstants.SIZEOF_SHA1;

			// update with recovery data
			while (pos < rend) {
				scratch.clear();
				if (rend - pos < scratch.capacity()) scratch.limit((int)(rend-pos));
				while (scratch.hasRemaining()) pos += store.read(pos, scratch);
				scratch.flip();
				sha1.update(scratch);
			}

			// update with zero for recovery hash
			sha1.update(ZERO_HASH);
			pos += ZERO_HASH.length;

			// update with count
			scratch.clear();
			scratch.limit(StorageConstants.SIZEOF_INT);
			while (scratch.hasRemaining()) pos += store.read(pos, scratch);
			scratch.flip();
			sha1.update(scratch);

			return sha1.digest();
		}
	}
}
